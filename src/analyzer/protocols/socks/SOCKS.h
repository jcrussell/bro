#ifndef socks_h
#define socks_h

// SOCKS v4 analyzer.

#include "analyzer/protocols/tcp/TCP.h"
#include "analyzer/protocols/pia/PIA.h"

namespace binpac  {
   namespace SOCKS {
	   class SOCKS_Conn;
   }
}

namespace analyzer { namespace socks {

class SOCKS_Analyzer : public tcp::TCP_ApplicationAnalyzer {
public:
	SOCKS_Analyzer(Connection* conn);
	~SOCKS_Analyzer();

	void EndpointDone(bool orig);

	virtual void Done();
	virtual void DeliverStream(int len, const u_char* data, bool orig);
	virtual void Undelivered(int seq, int len, bool orig);
	virtual void EndpointEOF(bool is_orig);

	static analyzer::Analyzer* InstantiateAnalyzer(Connection* conn)
		{ return new SOCKS_Analyzer(conn); }

protected:

	bool orig_done;
	bool resp_done;

	pia::PIA_TCP *pia;
	binpac::SOCKS::SOCKS_Conn* interp;
};

} } // namespace analyzer::* 

#endif