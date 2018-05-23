DB=enceladus
BUILD=${CURDIR}/build.sql
SCRIPTS=${CURDIR}/scripts
CSV='${CURDIR}/data/master_plan.csv'
INMS_CSV='${CURDIR}/data/INMS/inms.csv'
CDA_CSV='${CURDIR}/data/CDA/cda.csv'
MASTER=$(SCRIPTS)/import.sql
INMS_SQL=$(SCRIPTS)/inms_import.sql
CDA_SQL=$(SCRIPTS)/cda_import.sql
NORMALIZE=$(SCRIPTS)/normalize.sql

all: normalize
	psql $(DB) -f $(BUILD)

master:
	@cat $(MASTER) >> $(BUILD)
	@cat $(INMS_SQL) >> $(BUILD)
	@cat $(CDA_SQL) >> $(BUILD)

import: master
	@echo "COPY import.master_plan FROM $(CSV) WITH DELIMITER ',' HEADER CSV;" >> $(BUILD)
	@echo "COPY import.inms FROM $(INMS_CSV) WITH DELIMITER ',' HEADER CSV;" >> $(BUILD)
	@echo "COPY import.cda FROM $(CDA_CSV) WITH DELIMITER ',' HEADER CSV;" >> $(BUILD)

normalize: import
	@cat $(NORMALIZE) >> $(BUILD)

clean:
	@rm -rf $(BUILD)
