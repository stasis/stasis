void setup (void) { 
  remove("logfile.txt");
  remove("storefile.txt");
  remove("blob0_file.txt");
  remove("blob1_file.txt");
}

void teardown(void) {
  setup();
}

