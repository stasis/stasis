#ifndef __NETWORKSETUP_H
#define __NETWORKSETUP_H
/**
   This struct contains the state for the messages (networking) layer.
   Currently, everything here can be derived at startup, so this won't
   need to be in transactional storage, with some luck. */
typedef struct networkSetup {
  char * coordinator;
  unsigned short localport;
  char * localhost;
  int socket;
  /** 
      Same format as argv for main().  If a message is addressed to
      "broadcast", then the message will be sent to each
      "address:port" pair in this string.  If you want to use proper
      IP broadcast, then this list can simply contain one entry that
      contains a subnet broadcast address like "1.2.3.0:1234".

      It would be best to set this value to NULL and
      broadcast_list_count to zero if you don't plan to use broadcast.
  */
  char *** broadcast_lists;
  int broadcast_lists_count;
  int *broadcast_list_host_count;
} NetworkSetup;

/** This site is the coordinator. */
#define COORDINATOR -1
/** Obtain the hostnumber from the config file. */
#define DEFAULT_HOST -2
/** Parses the network configuration file. 
  @param  The name of the config file.
  @param  hostnumber COORDINATOR, or which subordinate this host is (zero indexed)
  @return an initialized NetworkSetup struct.
*/
NetworkSetup * readNetworkConfig(char * name, int hostnumber);

int consolidate_bc_groups(char *** list, NetworkSetup * ns) ;

#endif // __NETWORKSETUP_H
