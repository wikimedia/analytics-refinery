# Builds refinery diagrams
#
# Requires:
#   dia
#

DIA_FILES=oozie-overview.dia

DIA_PNG_TARGET_FILES=$(DIA_FILES:%.dia=%.png)

ALL_TARGET_FILES=$(DIA_PNG_TARGET_FILES)

all: $(ALL_TARGET_FILES)

clean:
	$(RM) $(ALL_TARGET_FILES) $(DIA_FILES:%.dia=%.dia~)

%.png: %.dia
	dia --export=$@ $<