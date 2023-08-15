M4_DESTINE_FILES = ime-1.1.m4 \
	ime-1.2.m4 \
	infiniband-0.1.m4 \
	lustre-1.8.9.m4 \
	lustre-2.1.6.m4 \
	lustre-2.4.2.m4 \
	lustre-2.5.m4 \
	lustre-ieel-2.5.m4 \
	lustre-ieel-2.7.m4 \
	lustre-es4-2.10.m4 \
	lustre-2.12.m4 \
	lustre-b_es5_1.m4 \
	lustre-b_es5_2.m4 \
	lustre-b_es6_0.m4 \
	lustre-b_es6_1.m4 \
	lustre-2.13.m4 \
	sfa-3.0.m4 \
	sfa-11.0.m4 \
	sfa-11.6.m4

M4_FILES = general.m4 lustre.m4 sfa.m4 sfa-11-general.m4 $(M4_DESTINE_FILES)

EXTRA_DIST = \
	grafana_dashboards \
	influxdb.conf.diff \
	$(M4_FILES)

XML_DESTINE_FILES=$(M4_DESTINE_FILES:.m4=.xml)

noinst_DATA = $(M4_FILES)

$(XML_DESTINE_FILES): $(M4_FILES)
	m4 $(@:.xml=.m4) > $@

%.xml_checked: %
	xmllint --valid --noout $< > /dev/null; \
	touch $@

CHECKS = $(XML_DESTINE_FILES:%=%.xml_checked)

clean-local:
	-rm -f $(XML_DESTINE_FILES) $(CHECKS)

all: all-am \
	$(XML_DESTINE_FILES) $(CHECKS)
