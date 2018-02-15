/**
* @file probemain.cpp
* @Author Leonce Mekinda
* @date September, 2016
* @brief Main file for the DNS Probe
*
* The program requires a MySQL database 
* to be created beforehand.
* The SQL statements for the required 
* schema are provided in this DBAccess class definition.
*
* To compile, type: "g++ -std=c++11 -o dnsprobe ProbeMain.cpp  -I /usr/include/mysql++ -I /usr/include/mysql -L/usr/lib -L/usr/local/lib -lmysqlpp -lldns"
*/

#include <iostream>
#include <memory>
#include <cstdlib>
#include <cstring>
#include <getopt.h>
#include "dnsprobe.h"

int Log::LOG_LEVEL = LOG_DEBUG;

int main(int argc, char* argv[]) {

  //Parse command-line parameters
  bool b_add_domains = false;
  bool b_delete_domains = false;
  dnsprobe::Time probe_interval = dnsprobe::DEFAULT_PROBE_INTERVAL;
  const char *dbname = dnsprobe::DEFAULT_DB_NAME;
  const char *username = dnsprobe::DEFAULT_USER_NAME;
  const char *password = dnsprobe::DEFAULT_PASSWORD;
  int ret = 0;
  opterr = 0;

  int c;
  while ((c = getopt (argc, argv, "adhb:p:u:t:v:")) != -1)
    switch (c) {
      case 'a':
        b_add_domains = true;
        break;
      case 'd':
        b_delete_domains = true;
        break;
      case 'b':
        dbname = optarg;
        break;
      case 'u':
        username = optarg;
        break;
      case 'p':
        password = optarg;
        break;
      case 't':
        probe_interval = atoi(optarg);
        break;
      case 'v':
        Log::LOG_LEVEL = atoi(optarg);
        break;
      case '?':
        if (optopt == 'b' || optopt == 'u' || optopt =='p'|| optopt == 't')
          std::cerr << "Option '-" << static_cast<char>(optopt) << "' requires an argument." << std::endl;
        else 
          std::cerr <<  "Unknown option `-" <<  static_cast<char>(optopt) << "'" << std::endl;
      default:
        ret = 1;
      case 'h':
        std::cerr << "\nFills a [dnsprobe] database with DNS probe statistics. Durations are in ms." << std::endl
                  << "+------------i----------------------------------------------------------------" << std::endl
                  << "Usage:\t" << argv[0] << " [-ad] [-b database] [-u username] [-p password] [-t probe_interval] [-v verbosity_level] [domain_1 ... domain_N]" << std::endl
                  << "\t-a: add all domains" << std::endl
                  << "\t-d: delete all domains" << std::endl
                  << "\t 0 = highest verbosity level, 1 = Lower (no debug messages) etc." << std::endl
                  << "+-----------------------------------------------------------------------------" << std::endl
                  << "Author: Leonce Mekinda <sites.google.com/site/leoncemekinda>\n" << std::endl;
        return ret;
    }

  // Manage domains (insertion / deletion)
  std::shared_ptr<dnsprobe::DBAccess> dbaccess(new dnsprobe::MySQLAccess);
  dbaccess->connect(dbname, username, password);

  dnsprobe::Domains domains;

  if (b_delete_domains) {
    // Delete every domain listed onthe comand line
    for (int index = optind; index < argc; index++)
      domains.push_back(dnsprobe::Domain(argv[index])); 
    
    dbaccess->deleteDomains(domains);

  } else if (b_add_domains) {

  dnsprobe::Domains old_domains;
  dbaccess->loadDomains(old_domains);

    // Insert new domains only
    for (int index = optind; index < argc; index++) {
      bool b_exists = false;
      for (const auto& domain : old_domains) {
        if (!strncmp(domain.getName().c_str(), argv[index], domain.getName().length())) {
          Log::write("Domain " + domain.getName() + " already in database.", Log::LOG_DEBUG, __FUNCTION__, __LINE__);
          b_exists = true;
          break;
        }
      }
      if (!b_exists) domains.push_back(dnsprobe::Domain(argv[index])); 
    }
    dbaccess->addDomains(domains);
  }


  // Launch Vantage point
  dnsprobe::Vantage::getInstance().start(dbaccess, probe_interval);

  dbaccess->disconnect();

  return ret;
}
