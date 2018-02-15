/**
* @file dnsprobe.h
* @Author Leonce Mekinda
* @date September, 2016
* @brief Header file for the DNS Probe
*
* The program requires a MySQL database 
* to be created beforehand.
* The SQL statements for the required 
* schema are provided in this DBAccess class.
*/

#ifndef DNSPROBE_H
#define DNSPROBE_H

#include <queue>
#include <ctime>
#include <sstream>
#include <stdexcept>
#include <memory>
#include <random>
#include <unordered_map>
#include <signal.h>
#include <sys/time.h>
#include "mysql++.h"
#include "logger.h"

extern "C" {
  #include "ldns/ldns.h"
}

namespace dnsprobe {

/// The Time in ms
typedef unsigned long Time;

//================================= Constants =======================================//

const Time   DEFAULT_PROBE_INTERVAL = 1000; //1s
const char*  DEFAULT_SERVER         = "localhost";
const char*  DEFAULT_DB_NAME        = "dnsprobe";
const char*  DEFAULT_USER_NAME      = "root";
const char*  DEFAULT_PASSWORD       = "";
const double DEFAULT_DB_UPDATE_FREQ = 4.;
const int    DEFAULT_DNS_RETRY      = 2;

//============================== Business objects ==================================//
/**
* @brief Probe event types
*/
typedef enum {
  EV_SEND_REQUEST,
  EV_RECV_DATA,
  EV_TIMEOUT,
  EV_ERROR,
} EventType; 

/**
* @brief Probe events
*/
struct Event {
 Time time;
 std::string target;
 EventType event;
 double duration;
};

typedef std::queue<Event> Events;

/**
* @brief The domain to be probed
*/
class Domain {

  size_t _rank;
  std::string _name;
  double _query_time_avg;
  double _query_time_stddev;
  size_t _query_count;
  Time _time_first;
  Time _time_last;
  Events _events;

  // Randomness
  std::default_random_engine _PRNG;
  std::uniform_int_distribution<int> _random_length = std::uniform_int_distribution<int>(4, 10);
  std::uniform_int_distribution<int> _random_char = std::uniform_int_distribution<int>(0, 35);

public:

/// Default constructor
  Domain(): _rank(0), _query_time_avg(0), _query_time_stddev(0), _query_count(0), _time_first(0), _time_last(0) {}
  
/// Constructor: ranks are automatically incremented by the db engine
  Domain(const std::string& name, size_t rank = 0, double query_time_avg = 0, double query_time_stddev = 0, double query_count = 0, double time_first = 0, double time_last = 0) :
    _rank(rank), _name(name), _query_time_avg(query_time_avg), 
    _query_time_stddev(query_time_stddev), _query_count(query_count), 
    _time_first(time_first), _time_last(time_last) {
    
    // Log object creation
    std::stringstream msg;
    msg << "Domain " << _name << " constructed with q_tm_avg =" << query_time_avg << " q_tm_stddev =" << query_time_stddev 
        << " q_count =" << query_count << " tm_first =" << time_first << " tm_last=" << time_last; 
    Log::write(msg.str(), Log::LOG_DEBUG, __FUNCTION__, __LINE__); 

    // Compute a hash value of the domain name
    const char *p = _name.c_str();
    unsigned char hash = 0;
    for (;*p; hash ^= *p++); 

    // Seed the random number generator with this hash
    _PRNG.seed(hash);
  }
 
  size_t getRank() const            { return _rank; }
  std::string getName() const       { return _name; }
  double getQueryTimeAvg() const    { return _query_time_avg; }
  double getQueryTimeStdDev() const { return _query_time_stddev; }
  size_t getQueryCount() const      { return _query_count; }
  Time getTimeFirst() const         { return _time_first; }
  Time getTimeLast() const          { return _time_last; }

/// Give access to inner events  
  Events& getEvents() { return _events; }
  
/// Update with events
  bool update(const Event& event)  { 

    // Save current event
    _events.push(event);
    
    if (event.event != EV_RECV_DATA) return false;

    // Update stats:

    if (!_time_first) _time_first = event.time;
    _time_last = event.time;
      
    double old_avg = _query_time_avg;

    // Update the sum of times
    _query_time_avg *= _query_count; 
    _query_time_avg += event.duration; 

     
     // Calculate the current sum of squared times
     double sqr_avg = old_avg * old_avg + _query_time_stddev * _query_time_stddev;
     sqr_avg *=  _query_count; 
     sqr_avg += event.duration * event.duration;

    _query_count++;

    //calculate the new mean and the quadratic mean
    _query_time_avg /= _query_count;
     sqr_avg /= _query_count;
    
      
    // Calculate the biased stddev from the avg and the quadratic avg
    // We do not apply the Bessel's correction to unbias the variance estimator.
    // The unbiased version can be obtained if needed by multiplying the variance by (n/n-1)     
     _query_time_stddev = sqr_avg - _query_time_avg * _query_time_avg;
     _query_time_stddev = sqrt(_query_time_stddev);

    return true;
  }

/// Create a random target in this domain
  std::string getRandomTarget() { 
    
    // Generate random strings of variable lengths
    std::string str;
    for (int target_len = _random_length(_PRNG); target_len > 0; target_len--) {
      unsigned char c = _random_char(_PRNG);
      if (c < 26) 
        str += 'a' + c;
      else 
        str += '0' + c - 26;
    }

    return str;
  }

  ~Domain() {}

};

typedef std::vector<Domain> Domains;

//================================= Database =========================================//
/**
* @brief DBAccess abstract class
*/
class DBAccess {

protected:
 std::string _dbname;
 std::string _username;
 std::string _password;

public:

  virtual bool connect(const char* dbname = 0, const char* username = 0, const char* password = 0) = 0; 
  virtual bool disconnect()                     = 0;
  virtual bool loadDomains(Domains& domains)    = 0;
  virtual bool addDomains(Domains& domains)     = 0;
  virtual bool deleteDomains(Domains& domains)  = 0;
  virtual bool saveDomains(Domains& domains)    = 0;
};

/**
* @brief MySQL Access implementation class
*/
class MySQLAccess: public DBAccess {

/**
* @brief Database creation code
* @code
* mysqladmin -u root create dnsprobe
* CREATE TABLE domain (
*   rank BIGINT AUTO_INCREMENT PRIMARY KEY, 
*   name VARCHAR(255) NOT NULL, 
*   query_time_avg DOUBLE, 
*   query_time_stddev DOUBLE, 
*   query_count BIGINT, 
*   time_first TIMESTAMP, 
*   time_last TIMESTAMP
* );
*
* CREATE TABLE measurement (
*   ID BIGINT AUTO_INCREMENT PRIMARY KEY, 
*   time TIMESTAMP, 
*   target VARCHAR(255) NOT NULL, 
*   type INT, 
*   duration_ms DOUBLE, 
*   domain_rank BIGINT NOT NULL, 
*   INDEX (domain_rank), 
*   FOREIGN KEY (domain_rank) REFERENCES domain(rank) ON DELETE CASCADE ON UPDATE CASCADE
* );
* @endcode
*/

mysqlpp::Connection _connection;
Domains _domains;

public:

/// Constructor creates the connection object without establishing the connection to the database server
  MySQLAccess(): _connection(bool(false)) {}

/// Connect to the database
  bool connect(const char* dbname = 0, const char* username = 0, const char* password = 0) throw (std::runtime_error) { 

    if (dbname)   _dbname   = dbname; 
    if (username) _username = username; 
    if (username) _username = username; 
    if (password) _password = password; 
    
    if (!_dbname.length()) {
      const char * message = "Database name is required. Exiting..";
      Log::write(message, Log::LOG_FATAL); 
      throw std::runtime_error(message);
    }

    //Connect to the MySQL server using given credentials
    if (!_connection.connect(dbname, DEFAULT_SERVER, username, password)) {

      std::stringstream msg;
      msg <<  "Cannot connect to " <<  DEFAULT_SERVER << "." << _dbname << " as " << _username;

      Log::write(msg.str(), Log::LOG_FATAL, __FUNCTION__, __LINE__); 

      throw std::runtime_error(msg.str());
    } 
        
    Log::write("Connected to " + _dbname + " as " + _username, Log::LOG_DEBUG, __FUNCTION__, __LINE__); 
    return true;
  }
  
/// Disconnect from the database
  bool disconnect() {
    _connection.disconnect(); 
    Log::write("Disconnected from " + _dbname, Log::LOG_DEBUG, __FUNCTION__, __LINE__); 
    return true;


  }

/// Load domains
  bool loadDomains(Domains& domains) {
    std::string sql = "SELECT rank, name, query_time_avg, query_time_stddev, query_count, UNIX_TIMESTAMP(time_first), UNIX_TIMESTAMP(time_last) FROM domain;";

    // Execute the SQL statement
    mysqlpp::Query query = _connection.query(sql);
    Log::write("Loading domains with query " + sql, Log::LOG_DEBUG, __FUNCTION__, __LINE__); 

    if (mysqlpp::StoreQueryResult results = query.store()) {
      for ( auto& row : results ) {
        domains.push_back(Domain(std::string(row[1]), size_t(row[0]), double(row[2]), double(row[3]), size_t(row[4]), size_t(row[5]), size_t(row[6])));
      }
          
    } else {
      
      std::stringstream msg;
      msg <<  "Failed to execute SQL statement: " << query.error();
      Log::write(msg.str(), Log::LOG_ERROR, __FUNCTION__, __LINE__); 
    }

  };

/// Add domains
  bool addDomains(Domains& domains)  {

    if (!domains.size()) return false; 

    std::stringstream sql;
    sql <<  "INSERT INTO domain (name, query_time_avg, query_time_stddev, query_count, time_first, time_last) VALUES \n";
    
    int i = 0;    
    for (const auto& domain : domains){
      if (i > 0) sql << ","; 
      sql << "('" << domain.getName() << "'," << domain.getQueryTimeAvg() << "," << domain.getQueryTimeStdDev() << ","
          << domain.getQueryCount() << ", FROM_UNIXTIME("  << domain.getTimeFirst() << "), FROM_UNIXTIME(" << domain.getTimeLast() << "))\n";
      i++;
    }
    sql << ";";

    Log::write("Inserting domains with query { " + sql.str() + " }", Log::LOG_DEBUG, __FUNCTION__, __LINE__); 

    // Execute the SQL statement
    mysqlpp::Query query = _connection.query(sql.str());
    if (! query.execute()) { 
      std::stringstream msg;
      msg <<  "Failed to execute SQL statement: " << query.error();
      Log::write(msg.str(), Log::LOG_ERROR, __FUNCTION__, __LINE__); 
    }

    return true;
  }

 /// Delete domains
  bool deleteDomains(Domains& domains) {

    if (!domains.size()) return false; 

    std::stringstream sql;
    sql <<  "DELETE FROM domain WHERE ";
    
    int i = 0;    
    for (const auto& domain : domains){
      if (i > 0) sql << " OR "; 
      sql << "( name='" << domain.getName()<< "')";
      i++;
    }
    sql << ";";

    Log::write("Deleting domains with query { " + sql.str() + " }", Log::LOG_DEBUG, __FUNCTION__, __LINE__); 

    // Execute the SQL statement
    mysqlpp::Query query = _connection.query(sql.str()); 
    if (! query.execute()) {
      std::stringstream msg;
      msg <<  "Failed to execute SQL statement: " << query.error();
      Log::write(msg.str(), Log::LOG_ERROR, __FUNCTION__, __LINE__); 
    }

    return true;
  }

 /// Update domains and insert measurements
  bool saveDomains(Domains& domains) {

    if (!domains.size()) return false; 
  
    // Update domains
    for (const auto& domain : domains) {

      std::stringstream sql;
      sql <<  "UPDATE domain SET name = '" << domain.getName() 
          << "', query_time_avg = " << domain.getQueryTimeAvg() << ", query_time_stddev = " << domain.getQueryTimeStdDev() 
          << ", query_count = " << domain.getQueryCount() 
          << ", time_first = FROM_UNIXTIME("  << domain.getTimeFirst() << "), time_last = FROM_UNIXTIME("  << domain.getTimeLast() << ") WHERE rank = " 
          << domain.getRank() << ";\n";
         
      Log::write("Updating domains with query { " + sql.str() + " }", Log::LOG_DEBUG, __FUNCTION__, __LINE__); 
    
      // Execute the SQL statement
      mysqlpp::Query query  = _connection.query(sql.str());
      if (! query.execute()) {
         std::stringstream msg;
        msg <<  "Failed to execute SQL statement: " << query.error();
        Log::write(msg.str(), Log::LOG_ERROR, __FUNCTION__, __LINE__); 
      }
    }

    // Insert measurements  
    std::stringstream sql;
    sql <<  "INSERT INTO measurement (time, target, type, duration_ms, domain_rank) VALUES \n";

    int i = 0;    
    for (auto& domain : domains) {
      while (!domain.getEvents().empty()) {

        auto event = domain.getEvents().front();
        if (i > 0) sql << ","; 
        sql << "(FROM_UNIXTIME(" << event.time << "),'" << event.target << "'," << event.event << "," << event.duration << "," << domain.getRank() << ")\n";
        i++;
        domain.getEvents().pop();
      } 
    }
    sql << ";";

    // Execute the SQL statement
    mysqlpp::Query query  = _connection.query(sql.str());
    if (! query.execute()) {
      
      std::stringstream msg;
      msg <<  "Failed to execute SQL statement: " << query.error();
      Log::write(msg.str(), Log::LOG_ERROR, __FUNCTION__, __LINE__); 
    }

    Log::write("Inserting measurements with query { " + sql.str() + " }", Log::LOG_DEBUG, __FUNCTION__, __LINE__); 

    return true;
  }
};

//============================== Network communication ==================================//
/**
* @brief Remote host reply
*/
struct Reply {
  Time time;
  std::string target;
  EventType event;
  double duration;
};


/**
* @brief Remote Query abstract class
*/
class RemoteQuery {

protected:

  std::shared_ptr<Domain> _p_domain;

public:

  RemoteQuery(Domain& domain): _p_domain(&domain) {}

/**
* @brief Send a query
*/
  virtual std::pair<Reply,bool> sendQuery()= 0;

/**
* @brief Probe a target
*/
  bool probe(){

    // Send query
    std::pair<Reply,bool> reply = sendQuery();
    
    // Process reply      
    if (!reply.second) {
      Log::write("Cannot send query to " + _p_domain->getName(), Log::LOG_ERROR, __FUNCTION__, __LINE__); 
    }
    
     // Update the domain
    _p_domain->update({reply.first.time, reply.first.target, reply.first.event, reply.first.duration}); 

    return  reply.second;
  }

  ~RemoteQuery(){}
};

typedef std::unordered_map<std::string, std::shared_ptr<RemoteQuery> > RemoteQueries;


/**
* @brief DNS Query implementation class
*/
class DNSQuery : public RemoteQuery {

  ldns_resolver* _ns_resolver;
  ldns_rdf* _ns_name;
  ldns_rr_list* _ns_addresses;

public:

  DNSQuery(Domain& domain) throw (std::runtime_error) : RemoteQuery(domain) { 
    // Initialize ldns variables
    _ns_name = ldns_dname_new_frm_str(_p_domain->getName().c_str());

    if (ldns_resolver_new_frm_file(&_ns_resolver, NULL) != LDNS_STATUS_OK) {
        const char * message = "Cannot create a resolver. Exiting..";
        Log::write(message, Log::LOG_FATAL, __FUNCTION__, __LINE__); 
        throw std::runtime_error(message);
    }
    
    // Set the number of retries
    ldns_resolver_set_retry(_ns_resolver, DEFAULT_DNS_RETRY);
  }

/**
* @brief Send a DNS query
*/
  std::pair<Reply,bool> sendQuery() {

    Reply reply;
    // By default the event is the request. It is updated by the reply if any.
    reply.target   = _p_domain->getRandomTarget() + "." + _p_domain->getName();
    reply.time     = time(0);
    reply.event    = EV_SEND_REQUEST;

    Log::write("Sending query for " + reply.target, Log::LOG_INFO, __FUNCTION__, __LINE__); 

    ldns_rdf* target_name = ldns_dname_new_frm_str(_p_domain->getName().c_str());

    ldns_pkt* packet = NULL;
    struct timespec start_time, end_time;
    
    // Measure query duration
    clock_gettime(CLOCK_REALTIME, &start_time);
    ldns_status query_status = ldns_resolver_query_status(&packet, _ns_resolver, target_name, LDNS_RR_TYPE_A, LDNS_RR_CLASS_CH, LDNS_RD);
    clock_gettime(CLOCK_REALTIME, &end_time);

    const double  SEC_TO_MILLI  = 1e+3;
    const double  MILLI_TO_NANO = 1e+3;
    
    // Set the duration in ms
    reply.duration = (end_time.tv_sec - start_time.tv_sec) * SEC_TO_MILLI  + (end_time.tv_nsec - start_time.tv_nsec) / MILLI_TO_NANO;

    if (packet && ldns_pkt_qr(packet)) {
        // If a packet was received
        reply.event = EV_RECV_DATA;

        std::string reply_ns_str;          
        if (query_status == LDNS_STATUS_OK) {
          // Update timestamp
          struct timeval timev = ldns_pkt_timestamp(packet);
          reply.time = timev.tv_sec; 
        
          //Update duration
          reply.duration  = ldns_pkt_querytime(packet);

          // Get name server name
          ldns_rdf* reply_ns =  ldns_pkt_answerfrom(packet);
          char* str = ldns_rdf2str(reply_ns);
          reply_ns_str = " from " + std::string(str);
          LDNS_FREE(str);
        }
        std::stringstream msg;
        msg << "Got answer" << reply_ns_str << " with status: { " << ldns_get_errorstr_by_id(query_status) << " } in " << reply.duration << " ms"; 
        Log::write(msg.str(), Log::LOG_INFO, __FUNCTION__, __LINE__); 

        ldns_pkt_free(packet);
    }

    ldns_rdf_deep_free(target_name); 

    return std::make_pair(reply, true);
  };

  ~DNSQuery() {
    // Free LDNS resources
    Log::write("Free LDNS resources for domain " + _p_domain->getName(), Log::LOG_DEBUG, __FUNCTION__, __LINE__); 
    for (;ldns_rdf *ns = ldns_resolver_pop_nameserver(_ns_resolver); ldns_rdf_deep_free(ns));
    ldns_rdf_deep_free(_ns_name); 
    ldns_resolver_deep_free(_ns_resolver); 
  }
};

  
//============================== Vantage Point ==================================//
/**
* @brief Local Vantage Point
* This class is a singleton
*/
class Vantage {

  Time _probe_interval;
  double _dbupdate_freq;
  Domains _domains;
  RemoteQueries _remoteQueries;
  std::shared_ptr<DBAccess> _dbaccess;
  bool _flag_stop;

  Vantage(){}


public:

  /// Launch Vantage point
  bool start(const std::shared_ptr<DBAccess>& dbaccess, Time probe_interval = DEFAULT_PROBE_INTERVAL, double dbupdate_freq = DEFAULT_DB_UPDATE_FREQ) {


    _dbaccess = dbaccess;
    _probe_interval = probe_interval;
    _dbupdate_freq = dbupdate_freq;
    _flag_stop = false;

    // Fetch domains from the database
    _dbaccess->loadDomains(_domains);

    if (!_domains.size()) {
      Log::write("No domain to probe.", Log::LOG_DEBUG, __FUNCTION__, __LINE__); 
      return false;
    }

    for (auto& domain : _domains) 
      _remoteQueries.insert(std::make_pair(domain.getName(), std::shared_ptr<RemoteQuery>(new DNSQuery(domain))));

    
    // Set signal handlers
    struct sigaction act;
    memset(&act, 0, sizeof(act));
    act.sa_handler = sigHandler;

    // Probe periodically
    sigaction(SIGALRM, &act, NULL); 
    
    // Save stats on interrupt
    sigaction(SIGINT , &act, NULL); 
    sigaction(SIGHUP, &act, NULL); 
    sigaction(SIGTERM, &act, NULL); 


    // Install the alarm
    struct itimerval itimer;
    itimer.it_value.tv_sec     = 
    itimer.it_interval.tv_sec  = _probe_interval / 1000; 
    itimer.it_value.tv_usec    = 
    itimer.it_interval.tv_usec = (_probe_interval % 1000) * 1000;
    
    setitimer(ITIMER_REAL, &itimer, NULL);

    probe();

    // Infinite loop
    for (;!_flag_stop;sleep(1)); 
  };


  /// Signal handler 
  static void sigHandler(int sig) {
    // Counts the number of SIGALRM for triggering buffer flush 
    static size_t alarm_counter = 0;

    switch(sig) {
      case SIGALRM: 
                    alarm_counter++;
                    if (alarm_counter >= getInstance()._dbupdate_freq) {
                      getInstance().save(); 
                      alarm_counter = 0;
                    }
                    getInstance().probe();
                    Log::write("SIGALRM fired", Log::LOG_DEBUG, __FUNCTION__, __LINE__);
                    break;
      case SIGINT : 
      case SIGHUP : 
      case SIGTERM: Log::write("Application interrupted." , Log::LOG_DEBUG, __FUNCTION__, __LINE__); 
                    getInstance().stop();
      default:      break;
    }
  }

  /// Save domains on interruption 
  void save() {
     _dbaccess->saveDomains(_domains);
  }

  /// Stop probing
  void stop() {
    save();
    _flag_stop = true;
  }

  /// Probe domains
  void probe() {
    Log::write("Probing all...", Log::LOG_DEBUG, __FUNCTION__, __LINE__);

    // Create Remote queries for every domain
    for (auto& remoteQuery : _remoteQueries)
      remoteQuery.second->probe();
    
  }

/**
* Return a reference to the Vantage point
*/
  static Vantage& getInstance() {
    static Vantage* instance = 0;
    return *((!instance)? (instance = new Vantage): instance);
  }

  ~Vantage() {}
};



}
#endif
