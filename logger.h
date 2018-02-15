/**
* @file logger.h
* @Author Leonce Mekinda
* @date September, 2016
* @brief Header file for the logger
*/

#ifndef LOGGER_H
#define LOGGER_H

#include <iostream>
#include <iomanip>
#include <unistd.h>
/**
* @brief Program Logger as a static class
*/
class Log {

public:
/**
* @brief Log message severity level
*/
  typedef enum {
    LOG_DEBUG,
    LOG_INFO,
    LOG_WARN,
    LOG_ERROR,
    LOG_FATAL
  } Severity;

protected:
/** 
* @brief private constructot:
* Logger cannot be instantiated
*/
  Log(){}

/** 
* @brief Maintain log size internally
*/
  static size_t getSize() {
    static size_t logSize = 0;
    return ++logSize;
  }

/**
* @brief Get a hulan-readable version of the severity
*/
  static const char* getSeverityAsChars(Severity severity){

    switch(severity){
      case LOG_DEBUG: return "DEBUG";   break;
      case LOG_INFO:  return "INFO";    break;
      case LOG_WARN:  return "WARNING"; break; 
      case LOG_ERROR: return "ERROR";   break;
      case LOG_FATAL: return "FATAL";   break;
      default:        return "Unknown severity";
    }
  }

public:

/**
* @brief Sets the minimum severity for being printed
*/
  static int LOG_LEVEL; 

  //Uncomment to feactivate logging:
  //static const int LOG_LEVEL = 255;
 
/**
* @brief Get current date and time
*/
  static std::string now() {
    std::time_t rawtime = std::time(0);
    std::string str = ctime(&rawtime);

    // Remove carriage return
    str.erase(str.length() - 1);

    return str;
  }

/**
* @brief Logger main function, writes logs to stderr
*/ 
  static void write(const std::string& message, Severity severity = LOG_INFO, const char* function = "", int line = 0){
    
    // Ignore message if the severity is not enough
    if (LOG_LEVEL > severity) return;
    
    // Print message otherwise
    std::cerr << "[" << std::setfill('0') << std::setw(6) << getSize() << "] " 
              << std::setw(15) << now() << " " << std::setfill(' ') << std::setw(8) << getSeverityAsChars(severity) << " \"" <<  message << "\""; 
    
    // Only print context if debug message
    if (severity == LOG_DEBUG) std::cerr << " in " << function << " at line " << line; 
    
    std::cerr << std::endl;
  }
  
};

#endif
