#include <libdfa/networksetup.h>
#include <confuse.h>
#include <stdlib.h>
#include <libdfa/messages.h>
#include <string.h>


/** @todo could validate ip addresses a bit better.  Currently, validate_host_port will accept 999.999.999.999:1
*/
int validate_host_port(cfg_t *cfg, cfg_opt_t *opt) {
  char * hostport = cfg_opt_getnstr(opt, cfg_opt_size(opt) - 1);
  if(strlen(hostport) > MAX_ADDRESS_LENGTH-1) {
    cfg_error(cfg, "string option %s ('%s') too long.\n\tMust be less than %d characters in section '%s'",
      opt->name, hostport, MAX_ADDRESS_LENGTH, cfg->name);
      return -1;
  }
  char * addr = parse_addr(hostport);
  if(addr == NULL) {
    cfg_error(cfg, "Error parsing address portion of string option %s ('%s').  should be of form nnn.nnn.nnn.nnn:mmmmm where n and m are intergers in section '%s'",
      opt->name, hostport, cfg->name);
      return -1;
  }
  int i;
  for(i = 0; i < strlen(addr); i++) {
    if((addr[i] < '0' || addr[i] > '9') && addr[i] != '.') {
       cfg_error(cfg, "Error parsing address from option %s ('%s'), should be IP address in section '%s'",
      opt->name, addr, cfg->name);
      return -1;
    }
  }
  free(addr);
  long int port = parse_port(hostport);
    
  if(port < 1 || port > 65535)
  {
     cfg_error(cfg, "port number must be between 1 and 65535 in option %s ('%s') section '%s'",
       opt->name, hostport, cfg->name);
       return -1;
  }
  return 0;
}

cfg_opt_t opts[] = {
  CFG_STR("coordinator", "127.0.0.1:10000", CFGF_NONE),
  CFG_STR_LIST("subordinates", 
    "{127.0.0.1:10001,127.0.0.1:10002,127.0.0.1:10003}", CFGF_NONE),
  CFG_END()
};
NetworkSetup * readNetworkConfig(char * name, int hostnumber) {

  cfg_t *cfg;

  cfg = cfg_init(opts, CFGF_NONE);
  cfg_set_validate_func(cfg, "coordinator",  validate_host_port);
  cfg_set_validate_func(cfg, "subordinates", validate_host_port);
  if(cfg_parse(cfg, name) == CFG_PARSE_ERROR) {
    fprintf(stderr, "Configuration file parse error.\n");
    fflush(NULL);
    return NULL;
  }
  printf("coordinator: %s\n", cfg_getstr(cfg, "coordinator"));
  printf("subordinate count: %d\n", cfg_size(cfg, "subordinates"));
  int i;
  for(i = 0; i < cfg_size(cfg, "subordinates"); i++) {
    printf("\t%s\n", cfg_getnstr(cfg, "subordinates", i));
  }
  
  if(hostnumber == COORDINATOR) {
    printf("I am coordinator\n");
  } else {
    printf("I am subordinate # %d\n", hostnumber);
  }
  
  cfg_free(cfg);
  return NULL;
}
