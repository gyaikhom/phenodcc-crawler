# Running and maintaining the PhenoDCC System

The following is the crawler directory structure. The crawler and database
settings are found in `/IMPC/bin/crawler/configs`. Example settings are
provided in this directory.

    PhenoDCC crawling infrastructure
    .
    ├── IMPC
    │   ├── bin
    │   │   └── crawler
    │   │       ├── build_annotations.sh
    │   │       ├── build_overviews.sh
    │   │       ├── configs
    │   │       │   ├── crontab.configs
    │   │       │   ├── live
    │   │       │   │   ├── phenodcc_context.properties
    │   │       │   │   ├── phenodcc_raw.properties
    │   │       │   │   ├── phenodcc_tracker.properties
    │   │       │   │   └── phenodcc_xmlvalidationresources.properties
    │   │       │   └── localhost
    │   │       │       ├── phenodcc_context.properties
    │   │       │       ├── phenodcc_raw.properties
    │   │       │       ├── phenodcc_tracker.properties
    │   │       │       └── phenodcc_xmlvalidationresources.properties
    │   │       ├── cron_job.sh
    │   │       ├── cron_logs
    │   │       │   └── crawler.log
    │   │       ├── exec_logs
    │   │       ├── execs
    │   │       │   ├── annotations
    │   │       │   │   └── 1.0
    │   │       │   │       ├── build_annotations.sh
    │   │       │   │       ├── create_temp_table.sql
    │   │       │   │       ├── IMPCAnnotationGenerator.jar
    │   │       │   │       ├── lib
    │   │       │   │       │   ├── commons-math3-3.1.1.jar
    │   │       │   │       │   ├── commons-math3-3.2.jar
    │   │       │   │       │   ├── mysql-connector-java-5.1.23-bin.jar
    │   │       │   │       │   ├── mysql-connector-java-5.1.24-bin.jar
    │   │       │   │       │   ├── REngine.jar
    │   │       │   │       │   └── RserveEngine.jar
    │   │       │   │       ├── README.TXT
    │   │       │   │       └── rename_table.sql
    │   │       │   ├── context
    │   │       │   │   └── phenodcc-context-2.0.0-jar-with-dependencies.jar
    │   │       │   ├── crawler
    │   │       │   │   └── phenodcc-crawler-1.4-jar-with-dependencies.jar
    │   │       │   ├── overviews
    │   │       │   │   └── 1.0
    │   │       │   │       ├── build_overviews.sh
    │   │       │   │       ├── create_genotype.sql
    │   │       │   │       ├── create_measurements_performed.sql
    │   │       │   │       ├── create_procedures_animals.sql
    │   │       │   │       ├── create_strains.sql
    │   │       │   │       ├── delete_inactive_records.sql
    │   │       │   │       ├── empty_genotype_and_strains.sql
    │   │       │   │       └── insert_metadata_groups.sql
    │   │       │   ├── serializer
    │   │       │   │   └── exportlibrary-xmlserializer-1.2.0-jar-with-dependencies.jar
    │   │       │   └── validator
    │   │       │       └── exportlibrary-xmlvalidator-1.2.1-jar-with-dependencies.jar
    │   │       ├── phenodcc-annotation -> execs/annotations/1.0/
    │   │       ├── phenodcc-context.jar -> execs/context/phenodcc-context-2.0.0-jar-with-dependencies.jar
    │   │       ├── phenodcc-crawler.jar -> execs/crawler/phenodcc-crawler-1.4-jar-with-dependencies.jar
    │   │       ├── phenodcc-overview -> execs/overviews/1.0/
    │   │       ├── phenodcc-serializer.jar -> execs/serializer/exportlibrary-xmlserializer-1.2.0-jar-with-dependencies.jar
    │   │       └── phenodcc-validator.jar -> execs/validator/exportlibrary-xmlvalidator-1.2.1-jar-with-dependencies.jar
    │   └── xml
    └── manual.pdf

