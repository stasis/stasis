#include <lladd/common.h>
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

cfg_opt_t group_opts[] = {
  CFG_INT_LIST("members", "{1,2,3}", CFGF_NONE),
  CFG_END()
};

cfg_opt_t opts[] = {
  CFG_STR("coordinator", "127.0.0.1:10000", CFGF_NONE),
  CFG_STR_LIST("subordinates", 
    "{127.0.0.1:10001,127.0.0.1:10002,127.0.0.1:10003}", CFGF_NONE),
  CFG_SEC("group", group_opts, CFGF_TITLE | CFGF_MULTI),
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
  DEBUG("coordinator: %s\n", cfg_getstr(cfg, "coordinator"));
  DEBUG("subordinate count: %d\n", cfg_size(cfg, "subordinates"));
  int i;
  for(i = 0; i < cfg_size(cfg, "subordinates"); i++) {
    DEBUG("\t%s\n", cfg_getnstr(cfg, "subordinates", i));
  }
  
  if(hostnumber == COORDINATOR) {
    DEBUG("I am coordinator\n");
  } else {
    DEBUG("I am subordinate # %d\n", hostnumber);
  }
  NetworkSetup * ret = calloc(1, sizeof(NetworkSetup));
  
  ret->localport = hostnumber == COORDINATOR 
		      ? parse_port(cfg_getstr(cfg, "coordinator"))
		      : parse_port(cfg_getnstr(cfg, "subordinates", hostnumber));
  ret->localhost = hostnumber == COORDINATOR
		      ? parse_addr(cfg_getstr(cfg, "coordinator"))
		      : parse_addr(cfg_getnstr(cfg, "subordinates", hostnumber));
  ret->socket    = -1; /// @todo where should the socket field be initialized?
  ret->broadcast_lists_count = cfg_size(cfg, "group");
  printf("broadcast list count = %d\n", ret->broadcast_lists_count);
  ret->broadcast_list_host_count = malloc(sizeof(int *) * ret->broadcast_lists_count);
  ret->broadcast_lists = malloc(sizeof(int**) * ret->broadcast_lists_count);
  for(i = 0; i < ret->broadcast_lists_count; i++) {
    cfg_t * group_cfg = cfg_getnsec(cfg, "group", i);
    
    int j;
    ret->broadcast_list_host_count[i] = cfg_size(group_cfg, "members");
    ret->broadcast_lists[i] = malloc(sizeof (int *) * ret->broadcast_list_host_count[i]);
    DEBUG("Group %d size %d\n", atoi(cfg_title(group_cfg)), ret->broadcast_list_host_count[i]);
    for(j = 0; j < ret->broadcast_list_host_count[i]; j++) {
       ret->broadcast_lists[i][j] = 
	  strdup(cfg_getnstr(cfg, "subordinates", cfg_getnint(group_cfg, "members", j)-1));
      
       DEBUG("\t->%s\n", ret->broadcast_lists[i][j]);
    }
  }
  
  cfg_free(cfg);
  return ret;
}
