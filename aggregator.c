#include <librdkafka/rdkafka.h>
#include <string.h>
#include <Judy.h>
#include <timeseries.h>
#include <assert.h>
#include <libipmeta.h>

#include <arpa/inet.h>
#include <errno.h>
#include <unistd.h>
#include <getopt.h>

#define DEFAULT_CONSUMER_TOPIC "tsk-production.graphite.active.ping-slash24.team-1.slash24test"
#define DEFAULT_PRODUCER_TOPIC "tsk-production.graphite.active.latencyloss"

#define DEFAULT_CONSUMER_GROUP "ap_slash24_aggregator"

#define IPMETA_INCLUDE_SLASH24_THRESHOLD 16

#define CONTINENT_COUNT 8
static const char *continent_strings[] = {
    "??", "AF", "AN", "AS", "EU", "NA", "OC", "SA",
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
    rd_kafka_t *rdk_conn;
    rd_kafka_topic_t *rdk_topic;
    int connected;
    int fatal_error;
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

} ipmeta_handle_t;

typedef struct cumulative_result {
    char *identifier;
    uint64_t latency;
    uint64_t probes_sent;
    uint64_t probes_lost;

    uint64_t s24_ips;       // used only for staging
} cumulative_result_t;

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

    return 0;
}

static void destroy_ipmeta(ipmeta_handle_t *ipm) {
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
    const char **country_continents;
    char key[512];
    int cnt, i;

    if (ipmeta_provider_get_all_records(ipm->provider_ipinfo, &records) == 0) {
        fprintf(stderr, "Error: IPMeta is reporting no IPInfo records are loaded\n");
        return -1;
    }
    free(records);

    reset_all_results(&(hdl->results.countries));

    cnt = ipmeta_provider_maxmind_get_iso2_list(&countries_iso2);

    for (i = 0; i < cnt; i++) {
        snprintf(key, 512, "country.%s", countries_iso2[i]);
        insert_single_result(&(hdl->results.countries), key);
    }

    return 0;
}


static void kafka_error_callback(rd_kafka_t *rk, int err, const char *reason,
        void *opaque) {

    kafka_consumer_handle_t *hdl = (kafka_consumer_handle_t *)opaque;

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

static void kafka_producer_error_callback(rd_kafka_t *rk, int err,
        const char *reason, void *opaque) {

    kafka_producer_handle_t *hdl = (kafka_producer_handle_t *)opaque;

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

    fprintf(stderr, "Kafka error detected in producer: %s (%d): %s\n",
            rd_kafka_err2str(err), err, reason);

}

int rdkafka_producer_connect(kafka_producer_handle_t *hdl) {

    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    char errorstr[512];

    rd_kafka_conf_set_opaque(conf, hdl);
    rd_kafka_conf_set_error_cb(conf, kafka_producer_error_callback);
    if (rd_kafka_conf_set(conf, "api.version.request", "true", errorstr,
                512) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Error setting kafka configuration: %s\n", errorstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    if (rd_kafka_conf_set(conf, "batch.num.messages", "100", errorstr,
                512) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Error setting kafka configuration: %s\n", errorstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    if (rd_kafka_conf_set(conf, "queue.buffering.max.ms", "500", errorstr,
                512) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Error setting kafka configuration: %s\n", errorstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    if (rd_kafka_conf_set(conf, "queue.buffering.max.messages", "500", errorstr,
                512) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Error setting kafka configuration: %s\n", errorstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }


    hdl->rdk_conn = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errorstr, 512);
    if (hdl->rdk_conn == NULL) {
        fprintf(stderr, "Error creating rdkafka producer: %s\n", errorstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    if (rd_kafka_brokers_add(hdl->rdk_conn, hdl->brokers) == 0) {
        fprintf(stderr, "Error adding brokers to rdkafka producer: %s\n",
                errorstr);
        return -1;
    }
    hdl->connected = 1;
    /* make sure our connection succeeded */
    rd_kafka_poll(hdl->rdk_conn, 5000);

    if (hdl->fatal_error) {
        return -1;
    }
    return 0;
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

    hdl->rdk_conn = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errorstr, 512);
    if (hdl->rdk_conn == NULL) {
        fprintf(stderr, "Error creating rdkafka consumer: %s\n", errorstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    if (rd_kafka_brokers_add(hdl->rdk_conn, hdl->brokers) == 0) {
        fprintf(stderr, "Error adding brokers to rdkafka consumer: %s\n",
                errorstr);
        return -1;
    }

    hdl->connected = 1;
    /* make sure our connection succeeded */
    rd_kafka_poll(hdl->rdk_conn, 5000);

    if (hdl->fatal_error) {
        return -1;
    }
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
                RD_KAFKA_OFFSET_BEGINNING) == -1) {
        fprintf(stderr, "Error while starting the kafka consumer: %s\n",
                rd_kafka_err2str(rd_kafka_last_error()));
        rd_kafka_topic_destroy(hdl->rdk_topic);
        hdl->rdk_topic = NULL;
        return -1;
    }

    return 0;
}

int rdkafka_producer_topic_connect(kafka_producer_handle_t *hdl) {
    if (hdl->fatal_error) {
        return -1;
    }
    hdl->rdk_topic = rd_kafka_topic_new(hdl->rdk_conn, hdl->topicname, NULL);
    if (hdl->rdk_topic == NULL) {
        fprintf(stderr, "Error while creating the kafka producer topic handle: %s\n",
                rd_kafka_err2str(rd_kafka_last_error()));
        return -1;
    }

    return 0;
}

static void commit_staged_results(Pvoid_t *staged, Pvoid_t *results) {

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

static void emit_results_to_kafka(kafka_producer_handle_t *hdl,
        Pvoid_t *map, uint64_t timestamp) {

    PWord_t pval;
    cumulative_result_t *cr;
    uint8_t index[512];

    index[0] = '\0';

    JSLF(pval, *map, index);
    while (pval) {
        cr = (cumulative_result_t *)(*pval);
        fprintf(stderr, "RESULT: %s %lu %lu %lu %lu\n",
                cr->identifier, timestamp, cr->latency, cr->probes_sent,
                cr->probes_lost);
        cr->latency = 0;
        cr->probes_sent = 0;
        cr->probes_lost = 0;

        JSLN(pval, *map, index);
    }

    fprintf(stderr, "------ DONE\n");
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

    if (timestamp != hdl->results.last_timestamp &&
            hdl->results.last_timestamp != 0) {

        /* end of round -- dump results and reset */
        emit_results_to_kafka(prodhdl, &(hdl->results.countries),
                hdl->results.last_timestamp);
    }
    hdl->results.last_timestamp = timestamp;

    if (s24_saddr != hdl->results.last_s24) {
        uint32_t swap_saddr = htonl(s24_saddr);

        ipmeta_record_set_clear(ipm->records);
        ipmeta_lookup_pfx(ipm->ipmeta, AF_INET,
                (void *)(&swap_saddr), 24, 0, ipm->records);

        commit_staged_results(&(hdl->staged.countries),
                &(hdl->results.countries));

        hdl->results.last_s24 = s24_saddr;
    }
    ipmeta_record_set_rewind(ipm->records);

    while ((rec = ipmeta_record_set_next(ipm->records, &num_ips))) {
        if (rec->country_code[0] != '\0') {
            snprintf(identifier, 512, "country.%s", rec->country_code);
            update_staged_result_map(&(hdl->staged.countries), identifier,
                    metric, num_ips, value);

        }
    }

    return 1;
}

int rdkafka_consume(kafka_consumer_handle_t *hdl,
        kafka_producer_handle_t *prodhdl, ipmeta_handle_t *ipm) {
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
                        if (strcmp(token3, "team-test") == 0) {
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

    fprintf(stderr, "Usage: %s -b <broker> -p <ipinfo config> -a <pfx2as config> [ other options ]\n", progname);
    fprintf(stderr, "\n");
    fprintf(stderr, "    -b <brokers>  --  set the Kafka broker for receiving prober results\n");
    fprintf(stderr, "    -p \"<config>\"  -- set the IPInfo configuration for libipmeta\n");
    fprintf(stderr, "    -a \"<config>\"  -- set the Prefix2AS configuration for libipmeta\n");
    fprintf(stderr, "    -g <groupname>   -- set the consumer group for kafka\n");
    fprintf(stderr, "    -t <topicname>   -- set the topic to consume messages from\n");
    fprintf(stderr, "    -T <topicname>   -- set the topic to write aggregated results into\n");
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

    while ((opt = getopt(argc, argv, "g:t:T:b:B:p:a:")) >= 0) {
        switch(opt) {
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
            default:
                usage(argv[0]);
        }
    }

    if (kafkahdl.brokers == NULL) {
        fprintf(stderr, "Please specify a set of kafka brokers using -b\n");
        exit(1);
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

        if (!prodhdl.connected) {
            if (rdkafka_producer_connect(&prodhdl) < 0) {
                sleep(5);
                continue;
            }
        }

        if (rdkafka_consume(&kafkahdl, &prodhdl, &ipm) < 0) {
            break;
        }
    }

    clear_results(&(kafkahdl.results.countries));
    destroy_ipmeta(&ipm);

    if (prodhdl.rdk_topic) {
        rd_kafka_topic_destroy(prodhdl.rdk_topic);
    }

    if (kafkahdl.rdk_topic) {
        rd_kafka_topic_destroy(kafkahdl.rdk_topic);
    }

    if (prodhdl.rdk_conn) {
        rd_kafka_destroy(prodhdl.rdk_conn);
    }

    if (kafkahdl.rdk_conn) {
        rd_kafka_consumer_close(kafkahdl.rdk_conn);
        rd_kafka_destroy(kafkahdl.rdk_conn);
    }

    return 0;
}
