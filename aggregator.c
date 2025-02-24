#include <librdkafka/rdkafka.h>
#include <string.h>
#include <Judy.h>
#include <timeseries.h>
#include <assert.h>
#include <libipmeta.h>
#include <timeseries.h>
#include <wandio.h>

#include <arpa/inet.h>
#include <sys/time.h>
#include <errno.h>
#include <unistd.h>
#include <getopt.h>

#define DEFAULT_CONSUMER_TOPIC "tsk-production.graphite.active.ping-slash24.team-1.slash24test"
#define DEFAULT_PRODUCER_TOPIC "graphite.active.latencyloss"

#define DEFAULT_CONSUMER_GROUP "ap_slash24_aggregator"

#define IPMETA_INCLUDE_SLASH24_THRESHOLD 16

#define CONTINENT_COUNT 8
static const char *continent_strings[] = {
    "??", "AF", "AN", "AS", "EU", "NA", "OC", "SA",
};

enum {
    RESULT_TYPE_GEO,
    RESULT_TYPE_ASN,
    RESULT_TYPE_GEOASN_COUNTRY,
    RESULT_TYPE_GEOASN_REGION,
};

typedef struct results {
    Pvoid_t continents;
    Pvoid_t countries;
    Pvoid_t regions;
    Pvoid_t asns;

    // TODO
    // geoasns
    //
    uint32_t last_s24;
    uint64_t last_timestamp;
} results_t;

typedef struct kafka_consumer_handle {
    char *brokers;
    char *topicname;
    char *consumer_group;
    char *teamname;

    rd_kafka_t *rdk_conn;
    rd_kafka_topic_t *rdk_topic;
    int connected;
    int fatal_error;

    results_t results;
    results_t staged;
} kafka_consumer_handle_t;

typedef struct kafka_producer_handle {
    char *brokers;
    char *topicname;
    int connected;
    int fatal_error;

    timeseries_t *timeseries;
    timeseries_kp_t *kp;
    uint32_t produced;

    time_t next_prov_write;
} kafka_producer_handle_t;


typedef struct ipmeta_handle {
    ipmeta_t *ipmeta;
    char *config_ipinfo;
    char *config_pfx2as;
    ipmeta_provider_t *provider_ipinfo;
    ipmeta_provider_t *provider_pfx2as;
    ipmeta_record_set_t *records;

    char *geoasn_csv_file;
    char *regions_csv_file;

    Pvoid_t known_regions;

} ipmeta_handle_t;

typedef struct cumulative_result {
    char *identifier;
    uint64_t latency;
    uint64_t probes_sent;
    uint64_t probes_lost;

    uint64_t s24_ips;       // used only for staging
} cumulative_result_t;


static int init_libtimeseries(kafka_producer_handle_t *hdl) {

    char tmpbuf[4096];
    timeseries_backend_t *backend;

    if (hdl->brokers == NULL) {
        fprintf(stderr, "No brokers specified for kafka output!\n");
        return -1;
    }

    if (hdl->topicname == NULL) {
        fprintf(stderr, "No topicname specified for kafka output!\n");
        return -1;
    }

    snprintf(tmpbuf, 4096, "-b %s -p tsk-production -c %s -f ascii",
            hdl->brokers, hdl->topicname);

    hdl->timeseries = timeseries_init();
    if (hdl->timeseries == NULL) {
        fprintf(stderr, "Unable to initialize timeseries_t\n");
        return -1;
    }

    backend = timeseries_get_backend_by_name(hdl->timeseries, "kafka");
    if (backend == NULL) {
        fprintf(stderr, "Kafka backend is not supported by your current version of libtimeseries\n");
        return -1;
    }

    if (timeseries_enable_backend(backend, tmpbuf) != 0) {
        fprintf(stderr, "Unable to enable Kafka backend with arguments '%s'\n",
                tmpbuf);
        return -1;
    }

    hdl->kp = timeseries_kp_init(hdl->timeseries,
                (TIMESERIES_KP_RESET | TIMESERIES_KP_DISABLE));
    if (!hdl->kp) {
        fprintf(stderr, "Unable to initialize key package for libtimeseries\n");
        return -1;
    }
    return 0;
}

static void destroy_libtimeseries(kafka_producer_handle_t *hdl) {
    if (hdl->kp) {
        timeseries_kp_free(&(hdl->kp));
    }
    if (hdl->timeseries) {
        timeseries_free(&(hdl->timeseries));
    }
}

static int load_ipmeta_regions(ipmeta_handle_t *ipm) {
    io_t *file;
    char buffer[2048];
    int read;
    char *tok;
    unsigned long reg_id;
    Word_t rc;

    if (ipm->regions_csv_file == NULL) {
        return -1;
    }

    if ((file = wandio_create(ipm->regions_csv_file)) == NULL) {
        fprintf(stderr, "ERROR: failed to open file '%s'\n",
                ipm->regions_csv_file);
        return -1;
    }

    while ((read = wandio_fgets(file, &buffer, 2048, 0)) > 0) {
        tok = strtok(buffer, ",");
        if (tok == NULL) {
            fprintf(stderr, "Malformed line in region csv file (should be id, fqid, name)\n");
            fprintf(stderr, "Line was: '%s'\n", buffer);
            goto failregioncsv;
        }

        if (strcmp(tok, "ioda_region_id") == 0) {
            continue;
        }
        errno = 0;
        reg_id = strtoul(tok, NULL, 10);
        if (errno) {
            fprintf(stderr, "Invalid value for IODA region ID: %s", tok);
            continue;
        }

        J1S(rc, ipm->known_regions, reg_id);
    }
    wandio_destroy(file);
    return 1;
failregioncsv:
    wandio_destroy(file);
    return -1;
}

static int init_ipmeta(ipmeta_handle_t *ipm) {

    char *provider_arg;

    ipm->ipmeta = ipmeta_init(IPMETA_DS_PATRICIA);
    if (ipm->ipmeta == NULL) {
        fprintf(stderr, "Error: could not initialize ipmeta\n");
        return -1;
    }

    if (ipm->config_ipinfo == NULL) {
        fprintf(stderr, "Error: null IPInfo provider configuration for ipmeta\n");
        return -1;
    }

    if (ipm->config_pfx2as == NULL) {
        fprintf(stderr, "Error: null pfx2as provider configuration for ipmeta\n");
        return -1;
    }

    provider_arg = ipm->config_ipinfo;

    ipm->provider_ipinfo = ipmeta_get_provider_by_id(ipm->ipmeta,
            IPMETA_PROVIDER_IPINFO);
    if (ipm->provider_ipinfo == NULL) {
        fprintf(stderr, "Error: unable to find IPInfo provider in libipmeta\n");
        return -1;
    }

    if (ipmeta_enable_provider(ipm->ipmeta, ipm->provider_ipinfo,
                provider_arg) != 0) {
        fprintf(stderr, "Error: failed to enable IPInfo provider: %s\n",
                provider_arg);
        return -1;
    }

    provider_arg = ipm->config_pfx2as;

    ipm->provider_pfx2as = ipmeta_get_provider_by_id(ipm->ipmeta,
            IPMETA_PROVIDER_PFX2AS);
    if (ipm->provider_pfx2as == NULL) {
        fprintf(stderr, "Error: unable to find pfx2as provider in libipmeta\n");
        return -1;
    }

    if (ipmeta_enable_provider(ipm->ipmeta, ipm->provider_pfx2as,
                provider_arg) != 0) {
        fprintf(stderr, "Error: failed to enable pfx2as provider: %s\n",
                provider_arg);
        return -1;
    }

    if ((ipm->records = ipmeta_record_set_init()) == NULL) {
        fprintf(stderr, "Error: failed to create IPMeta record set\n");
        return -1;
    }

    if (load_ipmeta_regions(ipm) < 0) {
        fprintf(stderr, "Error: failed to load regions CSV file\n");
        return -1;
    }

    struct timeval tv;
    gettimeofday(&tv, NULL);
    fprintf(stderr, "IPmeta init completed: %lu\n", tv.tv_sec);
    return 0;
}

static void destroy_ipmeta(ipmeta_handle_t *ipm) {
    int rc;

    J1FA(rc, ipm->known_regions);

    if (ipm->ipmeta) {
        ipmeta_free(ipm->ipmeta);
        ipm->ipmeta = NULL;
    }

    if (ipm->records) {
        ipmeta_record_set_free(&ipm->records);
        ipm->records = NULL;
    }
}

static void reset_all_results(Pvoid_t *map) {

    PWord_t pval;
    uint8_t index[512];

    index[0] = '\0';

    JSLF(pval, *map, index);
    while (pval) {
        cumulative_result_t *cr = (cumulative_result_t *)(*pval);
        cr->latency = 0;
        cr->probes_sent = 0;
        cr->probes_lost = 0;
        cr->s24_ips = 0;
        JSLN(pval, *map, index);
    }
}

static void clear_results(Pvoid_t *map) {
    PWord_t pval;
    Word_t rc;
    uint8_t index[512];

    index[0] = '\0';

    JSLF(pval, *map, index);
    while (pval) {
        cumulative_result_t *cr = (cumulative_result_t *)(*pval);
        if (cr) {
            if (cr->identifier) {
                free(cr->identifier);
            }
            free(cr);
        }
        JSLN(pval, *map, index);
    }
    JSLFA(rc, *map);
}


static void insert_single_result(Pvoid_t *map, char *identifier) {
    PWord_t pval;
    cumulative_result_t *cr;

    JSLI(pval, *map, (uint8_t *)identifier);
    if (*pval) {
        return;
    }
    cr = calloc(1, sizeof(cumulative_result_t));
    cr->identifier = strdup(identifier);
    cr->latency = 0;
    cr->probes_sent = 0;
    cr->probes_lost = 0;
    *pval = (Word_t)cr;
}

static int reset_results(kafka_consumer_handle_t *hdl, ipmeta_handle_t *ipm) {

    ipmeta_record_t **records;
    const char **countries_iso2;
    char key[512];
    int cnt, i, rc;
    Word_t index;

    if (ipmeta_provider_get_all_records(ipm->provider_ipinfo, &records) == 0) {
        fprintf(stderr, "Error: IPMeta is reporting no IPInfo records are loaded\n");
        return -1;
    }
    free(records);

    reset_all_results(&(hdl->results.continents));
    reset_all_results(&(hdl->results.countries));
    reset_all_results(&(hdl->results.regions));
    reset_all_results(&(hdl->results.asns));

    cnt = ipmeta_provider_maxmind_get_iso2_list(&countries_iso2);

    for (i = 0; i < cnt; i++) {
        snprintf(key, 512, "country.%s", countries_iso2[i]);
        insert_single_result(&(hdl->results.countries), key);
    }

    for (i = 0; i < CONTINENT_COUNT; i++) {
        snprintf(key, 512, "continent.%s", continent_strings[i]);
        insert_single_result(&(hdl->results.continents), key);
    }

    index = 0;
    J1F(rc, ipm->known_regions, index);
    while (rc != 0) {
        snprintf(key, 512, "region.%lu", index);
        insert_single_result(&(hdl->results.regions), key);
        J1N(rc, ipm->known_regions, index);
    }

    return 0;
}


static void kafka_error_callback(rd_kafka_t *rk, int err, const char *reason,
        void *opaque) {

    kafka_consumer_handle_t *hdl = (kafka_consumer_handle_t *)opaque;
    (void)rk;
    switch (err) {
        case RD_KAFKA_RESP_ERR__BAD_COMPRESSION:
        case RD_KAFKA_RESP_ERR__RESOLVE:
            hdl->fatal_error = 1;
            // fall through
        case RD_KAFKA_RESP_ERR__DESTROY:
        case RD_KAFKA_RESP_ERR__FAIL:
        case RD_KAFKA_RESP_ERR__TRANSPORT:
        case RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN:
            hdl->connected = 0;
            break;
    }

    fprintf(stderr, "Kafka error detected in consumer: %s (%d): %s\n",
            rd_kafka_err2str(err), err, reason);

}

int rdkafka_consumer_connect(kafka_consumer_handle_t *hdl) {
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    char errorstr[512];

    rd_kafka_conf_set_opaque(conf, hdl);
    rd_kafka_conf_set_error_cb(conf, kafka_error_callback);
    if (rd_kafka_conf_set(conf, "api.version.request", "true", errorstr,
                512) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Error setting kafka configuration: %s\n", errorstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    if (rd_kafka_conf_set(conf, "queued.min.messages", "60", errorstr,
                512) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Error setting kafka configuration: %s\n", errorstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    if (rd_kafka_conf_set(conf, "group.id", hdl->consumer_group, errorstr,
                512) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Error setting consumer group: %s\n", errorstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    if (rd_kafka_conf_set(conf, "bootstrap.servers", hdl->brokers,
                errorstr, 512) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Error setting bootstrap servers: %s\n", errorstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    if (rd_kafka_conf_set(conf, "enable.auto.commit", "true",
                errorstr, 512) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Error setting auto commit: %s\n", errorstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    if (rd_kafka_conf_set(conf, "auto.offset.reset", "earliest",
                errorstr, 512) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Error setting auto commit: %s\n", errorstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    hdl->rdk_conn = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errorstr, 512);
    if (hdl->rdk_conn == NULL) {
        fprintf(stderr, "Error creating rdkafka consumer: %s\n", errorstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    hdl->connected = 1;
    /* make sure our connection succeeded */
    rd_kafka_poll(hdl->rdk_conn, 5000);

    if (hdl->fatal_error) {
        return -1;
    }
    fprintf(stderr, "Kafka consumer connected to %s (%s)\n", hdl->brokers,
            hdl->consumer_group);
    return 0;
}


int rdkafka_topic_connect(kafka_consumer_handle_t *hdl) {

    if (hdl->fatal_error) {
        return -1;
    }

    hdl->rdk_topic = rd_kafka_topic_new(hdl->rdk_conn, hdl->topicname, NULL);
    if (hdl->rdk_topic == NULL) {
        fprintf(stderr, "Error while creating the kafka topic handle: %s\n",
                rd_kafka_err2str(rd_kafka_last_error()));
        return -1;
    }

    if (rd_kafka_consume_start(hdl->rdk_topic, 0,
                RD_KAFKA_OFFSET_STORED) == -1) {
        fprintf(stderr, "Error while starting the kafka consumer: %s\n",
                rd_kafka_err2str(rd_kafka_last_error()));
        rd_kafka_topic_destroy(hdl->rdk_topic);
        hdl->rdk_topic = NULL;
        return -1;
    }

    fprintf(stderr, "Started consuming from kafka topic: %s\n",
            hdl->topicname);
    return 0;
}

static void commit_staged_results_single_map(Pvoid_t *staged,
        Pvoid_t *results) {

    PWord_t pval, pval2;
    Word_t rc;
    cumulative_result_t *cr, *cr2;
    uint8_t index[512];

    index[0] = '\0';
    JSLF(pval, *staged, index);
    while(pval) {
        cr = (cumulative_result_t *)(*pval);

        if (cr->s24_ips >= IPMETA_INCLUDE_SLASH24_THRESHOLD) {
            JSLI(pval2, *results, index);
            if (*pval2 == 0) {
                *pval2 = (Word_t)cr;
            } else {
                cr2 = (cumulative_result_t *)(*pval2);
                cr2->latency += cr->latency;
                cr2->probes_sent += cr->probes_sent;
                cr2->probes_lost += cr->probes_lost;

            }
        } else {
            free(cr->identifier);
            free(cr);
        }
        JSLN(pval, *staged, index);
    }
    JSLFA(rc, *staged);
}

static void commit_staged_results(kafka_consumer_handle_t *hdl) {
    commit_staged_results_single_map(&(hdl->staged.continents),
            &(hdl->results.continents));
    commit_staged_results_single_map(&(hdl->staged.countries),
            &(hdl->results.countries));
    commit_staged_results_single_map(&(hdl->staged.regions),
            &(hdl->results.regions));
    commit_staged_results_single_map(&(hdl->staged.asns),
            &(hdl->results.asns));

}

static inline int update_timeseries_value(timeseries_kp_t *kp,
        char *identifier, char *teamname,
        uint8_t final, char *metric, uint8_t resulttype,
        uint64_t value) {

    char fulltskey[8192];
    int kval;

    if (resulttype == RESULT_TYPE_GEO) {
        snprintf(fulltskey, 8192, "active.ping-slash24.geo.ipinfo.%s.%s.%s.%s",
                identifier, teamname, final ? "final" : "provisional", metric);
    } else if (resulttype == RESULT_TYPE_ASN) {
        snprintf(fulltskey, 8192, "active.ping-slash24.%s.%s.%s.%s",
                identifier, teamname, final ? "final" : "provisional", metric);

    } else if (resulttype == RESULT_TYPE_GEOASN_COUNTRY) {

    } else if (resulttype == RESULT_TYPE_GEOASN_REGION) {

    } else {
        return -1;
    }


    kval = timeseries_kp_get_key(kp, fulltskey);
    if (kval == -1) {
        kval = timeseries_kp_add_key(kp, fulltskey);
    } else {
        timeseries_kp_enable_key(kp, kval);
    }
    if (kval == -1) {
        fprintf(stderr, "Warning: unable to add timeseries key for %s\n",
                fulltskey);
        return -1;
    }
    timeseries_kp_set(kp, kval, value);
    return 0;
}

static void emit_result_map_to_kafka(kafka_producer_handle_t *hdl,
        Pvoid_t *map, char *teamname, uint8_t final, uint8_t resulttype) {

    PWord_t pval;
    cumulative_result_t *cr;
    uint8_t index[512];
    uint64_t addval = 0;
    double tmp;
    struct timeval tv;

    index[0] = '\0';

    gettimeofday(&tv, NULL);
    JSLF(pval, *map, index);
    while (pval) {
        cr = (cumulative_result_t *)(*pval);

        /* TODO add other statistics for latency, e.g. median, percentiles */

        if (cr->probes_sent > 0) {
            if (cr->latency > 0 && cr->probes_lost < cr->probes_sent) {
                addval = (uint64_t)(cr->latency /
                        (cr->probes_sent - cr->probes_lost));
                if (update_timeseries_value(hdl->kp, cr->identifier,
                            teamname, final, "mean_latency", resulttype,
                            addval) < 0) {
                    return;
                }
            }

            tmp = ((double)cr->probes_lost / cr->probes_sent) * 10000;
            if (update_timeseries_value(hdl->kp, cr->identifier, teamname,
                        final, "loss_pct", resulttype, (uint64_t)(tmp)) < 0) {
                return;
            }
        }

        if (update_timeseries_value(hdl->kp, cr->identifier, teamname, final,
                "probe_count", resulttype, cr->probes_sent) < 0) {
            return;
        }

        if (final) {
            cr->latency = 0;
            cr->probes_sent = 0;
            cr->probes_lost = 0;
            cr->s24_ips = 0;
        }

        JSLN(pval, *map, index);
    }

}

static void emit_results_to_kafka(kafka_consumer_handle_t *hdl,
        kafka_producer_handle_t *prodhdl, uint8_t final) {

    emit_result_map_to_kafka(prodhdl, &(hdl->results.continents),
            hdl->teamname, final, RESULT_TYPE_GEO);
    emit_result_map_to_kafka(prodhdl, &(hdl->results.countries),
            hdl->teamname, final, RESULT_TYPE_GEO);
    emit_result_map_to_kafka(prodhdl, &(hdl->results.regions),
            hdl->teamname, final, RESULT_TYPE_GEO);
    emit_result_map_to_kafka(prodhdl, &(hdl->results.asns),
            hdl->teamname, final, RESULT_TYPE_ASN);
}

static void update_staged_result_map(Pvoid_t *map, char *identifier,
        char *metric, uint64_t num_ips, uint64_t value) {

    PWord_t pval;
    cumulative_result_t *cr;

    JSLI(pval, *map, (uint8_t *)identifier);
    if (*pval == 0) {
        cr = calloc(1, sizeof(cumulative_result_t));
        cr->identifier = strdup(identifier);
        *pval = (Word_t)cr;
    } else {
        cr = (cumulative_result_t *)(*pval);
    }

    if (strcmp(metric, "latency") == 0) {
        if (cr->s24_ips == 0) {
            cr->latency = value;
        }
        cr->s24_ips += num_ips;
    } else if (strcmp(metric, "probessent") == 0) {
        if (cr->probes_sent == 0) {
            cr->probes_sent = value;
        }
    } else if (strcmp(metric, "lostprobes") == 0) {
        if (cr->probes_lost == 0) {
            cr->probes_lost = value;
        }
    }

}

static int process_received_result(kafka_consumer_handle_t *hdl,
        kafka_producer_handle_t *prodhdl,
        ipmeta_handle_t *ipm, char *s24_key, char *metric, char *valstr,
        char *timestr) {

    uint32_t s24_saddr;
    uint64_t timestamp, value, num_ips;
    ipmeta_record_t *rec;
    char identifier[512];
    struct timeval tv;

    errno = 0;
    s24_saddr = strtoul(s24_key, NULL, 10);
    if (errno != 0) {
        fprintf(stderr, "Unable to convert slash24 into a numeric value: %s\n", s24_key);
        return -1;
    }

    errno = 0;
    timestamp = strtoul(timestr, NULL, 10);
    if (errno != 0) {
        fprintf(stderr, "Unable to convert timestamp into a numeric value: %s\n", timestr);
        return -1;
    }
    errno = 0;
    value = strtoul(valstr, NULL, 10);
    if (errno != 0) {
        fprintf(stderr, "Unable to convert value into a numeric value: %s\n", valstr);
        return -1;
    }

    if (timestamp < hdl->results.last_timestamp) {
        /* old timestamp, nothing we can really do about it now... */
        return 0;
    }

    if (timestamp != hdl->results.last_timestamp) {
        gettimeofday(&tv, NULL);
        fprintf(stderr, "Started processing timestamp %lu at %lu\n",
                timestamp, tv.tv_sec);
        prodhdl->next_prov_write = tv.tv_sec + 30;
    }
    if (timestamp != hdl->results.last_timestamp &&
            hdl->results.last_timestamp != 0) {

        /* end of round -- dump results and reset */
        fprintf(stderr,
                "%lu -- Generating final results for round %u (%lu)\n",
                tv.tv_sec, prodhdl->produced + 1, hdl->results.last_timestamp);
        commit_staged_results(hdl);
        emit_results_to_kafka(hdl, prodhdl, 1);
        timeseries_kp_flush(prodhdl->kp, hdl->results.last_timestamp);
        gettimeofday(&tv, NULL);
        fprintf(stderr,
                "%lu -- Flushed results for round %u (%lu)\n", tv.tv_sec,
               prodhdl->produced + 1, hdl->results.last_timestamp);
        prodhdl->produced ++;
        reset_results(hdl, ipm);
    }
    hdl->results.last_timestamp = timestamp;

    if (s24_saddr != hdl->results.last_s24) {
        uint32_t swap_saddr = htonl(s24_saddr);

        ipmeta_record_set_clear(ipm->records);
        ipmeta_lookup_pfx(ipm->ipmeta, AF_INET,
                (void *)(&swap_saddr), 24, 0, ipm->records);

        commit_staged_results(hdl);
        hdl->results.last_s24 = s24_saddr;
    }
    ipmeta_record_set_rewind(ipm->records);

    while ((rec = ipmeta_record_set_next(ipm->records, &num_ips))) {
        if (rec->country_code[0] != '\0') {
            snprintf(identifier, 512, "country.%s", rec->country_code);
            update_staged_result_map(&(hdl->staged.countries), identifier,
                    metric, num_ips, value);

        }

        if (rec->continent_code[0] != '\0') {
            snprintf(identifier, 512, "continent.%s", rec->continent_code);
            update_staged_result_map(&(hdl->staged.continents), identifier,
                    metric, num_ips, value);
        }

        if (rec->region_code != 0) {
            snprintf(identifier, 512, "region.%d", rec->region_code);
            update_staged_result_map(&(hdl->staged.regions), identifier,
                    metric, num_ips, value);
        }
        if (rec->asn_cnt > 0) {
            // ignore ASN groups for now
            snprintf(identifier, 512, "asn.%u", rec->asn[0]);
            update_staged_result_map(&(hdl->staged.asns), identifier, metric,
                    num_ips, value);
        }

    }

    return 1;
}

int rdkafka_consume(kafka_consumer_handle_t *hdl,
        kafka_producer_handle_t *prodhdl, ipmeta_handle_t *ipm) {
    struct timeval tv;
    if (hdl->rdk_topic == NULL && rdkafka_topic_connect(hdl) < 0) {
        sleep(1);
        return -1;
    }

    while (hdl->connected) {
        rd_kafka_message_t *msg;
        char *lineptr = NULL;
        char *spaceptr = NULL;
        char *dotptr = NULL;
        char *token, *token2, *token3;

        rd_kafka_poll(hdl->rdk_conn, 0);

        if (hdl->fatal_error) {
            return -1;
        }

        msg = rd_kafka_consume(hdl->rdk_topic, 0, 1000);
        if (msg == NULL) {
            if (errno != ETIMEDOUT) {
                fprintf(stderr,
                        "Error while retrieving message from kafka: %s\n",
                        strerror(errno));
                return -1;
            }
            gettimeofday(&tv, NULL);
            if (prodhdl->next_prov_write != 0 &&
                    tv.tv_sec >= prodhdl->next_prov_write) {
                fprintf(stderr,
                        "%lu: writing provisional results for %lu\n",
                        tv.tv_sec, hdl->results.last_timestamp);
                commit_staged_results(hdl);
                emit_results_to_kafka(hdl, prodhdl, 0);
                timeseries_kp_flush(prodhdl->kp, hdl->results.last_timestamp);
                gettimeofday(&tv, NULL);
                fprintf(stderr,
                        "%lu: flushed provisional results for %lu\n",
                        tv.tv_sec, hdl->results.last_timestamp);
                prodhdl->next_prov_write = 0;
            }
            continue;
        }

        if (msg->err ==  RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            rd_kafka_message_destroy(msg);
            continue;
        }

        if (msg->err != 0) {
            fprintf(stderr, "Error while consuming kafka message: %s\n",
                    rd_kafka_message_errstr(msg));
            rd_kafka_message_destroy(msg);
            rd_kafka_topic_destroy(hdl->rdk_topic);
            rd_kafka_destroy(hdl->rdk_conn);
            hdl->rdk_topic = NULL;
            hdl->rdk_conn = NULL;
            hdl->connected = 0;
            break;
        }

        char *key, *valuestr, *timestr, *lastkey, *s24key;
        uint8_t key_skip;

        // TODO
        //  * split message payload into individual lines
        //  * discard all lines with unrelated keys
        //    * including team-2 results during dev/testing
        //  * start thinking about the per-geo aggregation
        token = strtok_r((char *)msg->payload, "\n", &lineptr);
        while (token) {
            token2 = strtok_r(token, " ", &spaceptr);
            key = NULL;
            valuestr = NULL;
            timestr = NULL;
            key_skip = 1;
            s24key = NULL;
            lastkey = NULL;

            while(token2) {
                if (key == NULL) {
                    key = token2;
                    token3 = strtok_r(token2, ".", &dotptr);

                    while(token3) {
                        if (strcmp(token3, hdl->teamname) == 0) {
                            key_skip = 0;
                        }
                        if (strncmp(token3, "__S24_", 6) == 0) {
                            s24key = token3 + 6;
                        }

                        lastkey = token3;
                        token3 = strtok_r(NULL, ".", &dotptr);
                    }
                } else if (valuestr == NULL) {
                    valuestr = token2;
                } else if (timestr == NULL) {
                    timestr = token2;
                }
                token2 = strtok_r(NULL, " ", &spaceptr);
            }

            if (key_skip || !lastkey || !s24key) {
                goto nextline;
            }

            if (!key || !valuestr || !timestr) {
                fprintf(stderr, "Line does not appear to have the right format?\n");
                goto nextline;
            }

            if (process_received_result(hdl, prodhdl, ipm, s24key, lastkey,
                    valuestr, timestr) < 0) {
                rd_kafka_message_destroy(msg);
                rd_kafka_consume_stop(hdl->rdk_topic, 0);
                return -1;
            }


nextline:
            token = strtok_r(NULL, "\n", &lineptr);
        }

        rd_kafka_message_destroy(msg);
    }
    rd_kafka_consume_stop(hdl->rdk_topic, 0);
    return 0;
}

static void usage(char *progname) {

    fprintf(stderr, "Usage: %s -b <broker> -p <ipinfo config> -a <pfx2as config> -R <regions csv> [ other options ]\n", progname);
    fprintf(stderr, "\n");
    fprintf(stderr, "    -b <brokers>  --  set the Kafka broker for receiving prober results\n");
    fprintf(stderr, "    -p \"<config>\"  -- set the IPInfo configuration for libipmeta\n");
    fprintf(stderr, "    -a \"<config>\"  -- set the Prefix2AS configuration for libipmeta\n");
    fprintf(stderr, "    -g <groupname>   -- set the consumer group for kafka\n");
    fprintf(stderr, "    -M \"<teamname>\"    -- only process measurements from probers belonging to \n    this team (default: team-2)\n");
    fprintf(stderr, "    -t <topicname>   -- set the topic to consume messages from\n");
    fprintf(stderr, "    -T <topicname>   -- set the topic to write aggregated results into\n");
    fprintf(stderr, "    -R <filepath>    -- path to the regions CSV file\n");
    fprintf(stderr, "    -B <brokers>     -- set the Kafka broker for producing aggregated results\n");
    fprintf(stderr, "                        (if different from the broker set using '-b'\n");


}

int main(int argc, char **argv) {
    int opt;

    kafka_consumer_handle_t kafkahdl;
    ipmeta_handle_t ipm;
    kafka_producer_handle_t prodhdl;

    memset(&kafkahdl, 0, sizeof(kafkahdl));
    memset(&prodhdl, 0, sizeof(prodhdl));
    memset(&ipm, 0, sizeof(ipm));

    while ((opt = getopt(argc, argv, "R:M:g:t:T:b:B:p:a:")) >= 0) {
        switch(opt) {
            case 'M':
                kafkahdl.teamname = optarg;
                break;
            case 'g':
                kafkahdl.consumer_group = optarg;
                break;
            case 't':
                kafkahdl.topicname = optarg;
                break;
            case 'T':
                prodhdl.topicname = optarg;
                break;
            case 'b':
                kafkahdl.brokers = optarg;
                break;
            case 'B':
                prodhdl.brokers = optarg;
                break;
            case 'p':
                ipm.config_ipinfo = optarg;
                break;
            case 'a':
                ipm.config_pfx2as = optarg;
                break;
            case 'R':
                ipm.regions_csv_file = optarg;
                break;

            default:
                usage(argv[0]);
        }
    }

    if (kafkahdl.brokers == NULL) {
        fprintf(stderr, "Please specify a set of kafka brokers using -b\n");
        exit(1);
    }

    if (kafkahdl.teamname == NULL) {
        kafkahdl.teamname = "team-2";
    }

    if (prodhdl.brokers == NULL) {
        prodhdl.brokers = kafkahdl.brokers;
    }

    if (kafkahdl.topicname == NULL) {
        kafkahdl.topicname = DEFAULT_CONSUMER_TOPIC;
    }

    if (prodhdl.topicname == NULL) {
        prodhdl.topicname = DEFAULT_PRODUCER_TOPIC;
    }

    if (kafkahdl.consumer_group == NULL) {
        kafkahdl.consumer_group = DEFAULT_CONSUMER_GROUP;
    }

    if (ipm.config_ipinfo == NULL) {
        fprintf(stderr, "Please specify the IPInfo configuration using -p\n");
        exit(1);
    }

    if (ipm.config_pfx2as == NULL) {
        fprintf(stderr, "Please specify the Prefix2AS configuration using -a\n");
        exit(1);
    }

    if (ipm.regions_csv_file == NULL) {
        fprintf(stderr, "Please specify the path to the regions CSV file using -R\n");
        exit(1);
    }

    if (init_libtimeseries(&prodhdl) < 0) {
        fprintf(stderr, "Failed to initialize libtimeseries for kafka output\n");
        exit(1);
    }

    if (init_ipmeta(&ipm) < 0) {
        fprintf(stderr, "IPMeta configuration failed\n");
        exit(1);
    }

    reset_results(&kafkahdl, &ipm);

    while (1) {
        if (!kafkahdl.connected) {
            if (rdkafka_consumer_connect(&kafkahdl) < 0) {
                sleep(5);
                continue;
            }
        }

        if (rdkafka_consume(&kafkahdl, &prodhdl, &ipm) < 0) {
            break;
        }
    }

    clear_results(&(kafkahdl.results.asns));
    clear_results(&(kafkahdl.results.regions));
    clear_results(&(kafkahdl.results.countries));
    clear_results(&(kafkahdl.results.continents));
    destroy_ipmeta(&ipm);
    destroy_libtimeseries(&prodhdl);

    if (kafkahdl.rdk_topic) {
        rd_kafka_topic_destroy(kafkahdl.rdk_topic);
    }

    if (kafkahdl.rdk_conn) {
        rd_kafka_consumer_close(kafkahdl.rdk_conn);
        rd_kafka_destroy(kafkahdl.rdk_conn);
    }

    return 0;
}
