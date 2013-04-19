
#include "plugin/Plugin.h"

#include "SMTP.h"

BRO_PLUGIN_BEGIN(SMTP)
	BRO_PLUGIN_DESCRIPTION("SMTP Analyzer");
	BRO_PLUGIN_ANALYZER("SMTP", smtp::SMTP_Analyzer);
	BRO_PLUGIN_BIF_FILE(events);
	BRO_PLUGIN_BIF_FILE(functions);
BRO_PLUGIN_END