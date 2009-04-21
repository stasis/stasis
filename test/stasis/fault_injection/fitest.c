/*---
This software is copyrighted by the Regents of the University of
California and Ashok Sudarsanam. The following terms apply to all files
associated with the software unless explicitly disclaimed in
individual files.
                                                                                                                                  
The authors hereby grant permission to use, copy, modify, distribute,
and license this software and its documentation for any purpose,
provided that existing copyright notices are retained in all copies
and that this notice is included verbatim in any distributions. No
written agreement, license, or royalty fee is required for any of the
authorized uses. Modifications to this software may be copyrighted by
their authors and need not follow the licensing terms described here,
provided that the new terms are clearly indicated on the first page of
each file where they apply.
                                                                                                                                 
IN NO EVENT SHALL THE AUTHORS OR DISTRIBUTORS BE LIABLE TO ANY PARTY
FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES
ARISING OUT OF THE USE OF THIS SOFTWARE, ITS DOCUMENTATION, OR ANY
DERIVATIVES THEREOF, EVEN IF THE AUTHORS HAVE BEEN ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
                                                                                                                                  
THE AUTHORS AND DISTRIBUTORS SPECIFICALLY DISCLAIM ANY WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
NON-INFRINGEMENT. THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, AND
THE AUTHORS AND DISTRIBUTORS HAVE NO OBLIGATION TO PROVIDE
MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
                                                                                                                                  
GOVERNMENT USE: If you are acquiring this software on behalf of the
U.S. government, the Government shall have only "Restricted Rights" in
the software and related documentation as defined in the Federal
Acquisition Regulations (FARs) in Clause 52.227.19 (c) (2). If you are
acquiring the software on behalf of the Department of Defense, the
software shall be classified as "Commercial Computer Software" and the
Government shall have only "Restricted Rights" as defined in Clause
252.227-7013 (c) (1) of DFARs. Notwithstanding the foregoing, the
authors grant the U.S. Government and others acting in its behalf
permission to use and distribute the software in accordance with the
terms specified in this license.
---*/


#include "fitest.h"

testMode currentMode;

int iteration;
int baseKey;
int numThreads;
int opsPerThread;
int opsPerTransaction;
int mallocsBeforeFailure;
int logWritesBeforeFailure;
int pageWritesBeforeFailure;
int logWriteAmountDuringFailure;
int pageWriteAmountDuringFailure;


int main(int argc, char** argv) {
  currentMode = INVALID_MODE;

  if (argc == 1) {
    currentMode = MASTER_MODE;
  }
  else {
    if(!strcmp("-d",argv[1])) {
      currentMode = INTERACTIVE_MODE;
    } else {
      currentMode = (testMode) atoi(argv[1]);
    }
  }
  int err = 0;
  switch (currentMode) {
  case MASTER_MODE:
    err = runMaster(argv[0],0);
    break;

  case INTERACTIVE_MODE:
    err = runMaster(argv[0],1);
    break;

  case SUBJECT_MODE:
    err = runSubject();
    break;

  case CHECKER_MODE:
    err = runChecker();
    break;

  default:
    printf("Usage: %s [-d]\n  -d: Run interactively (facilitates debugging)\n", argv[0]);
    err = 1;
  }

  return err;
}

int
runMaster(const char* execname, int interactive) {
  pid_t pid;
  int numIters, retval, errorFound = 0;
  char modeBuf[20];
  FILE* configFile;

  int fail = 0;

  /* Remove previously generated text files. */
  unlink("logfile.txt");
  unlink("storefile.txt");
  unlink("inserts.txt");
  unlink("commits.txt");
  unlink("recordid.txt");
  unlink("iterinfo.txt");

  configFile = fopen("config.txt", "r");
  fscanf(configFile, "num_iterations = %d\n\n", &numIters);

  baseKey = 0;

  for (iteration = 0; iteration < numIters; iteration++) {

    printf("*** Testing <Iteration %d> ***\n", iteration); fflush(stdout);

    printf("\tRunning Master Process\n"); fflush(stdout);

    /* Read in the configuration for the current iteration and write to the 
     * file "iter.txt".  We will use a shared buffer, instead of a file, to
     * allow the various processes to communicate with each other.
     */
    fscanf(configFile, "num_threads = %d\n", &numThreads);
    fscanf(configFile, "ops_per_thread = %d\n", &opsPerThread);
    fscanf(configFile, "ops_per_transaction = %d\n", &opsPerTransaction);
    fscanf(configFile, "mallocs_before_failure = %d\n", &mallocsBeforeFailure);
    fscanf(configFile, "log_writes_before_failure = %d (%d)\n", 
	   &logWritesBeforeFailure, &logWriteAmountDuringFailure);
    fscanf(configFile, "page_writes_before_failure = %d (%d)\n\n", 
	   &pageWritesBeforeFailure, &pageWriteAmountDuringFailure);

    writeGlobalsToSharedBuffer();

    /* Fork a new process for the 'Subject'. */
    pid = fork();
    
    if (pid == 0) {
      /* We are in the subject process.  */
      sprintf(modeBuf, "%d", (int) SUBJECT_MODE);
      if(!interactive) {
        retval = execl(execname, execname, modeBuf, (char*) 0);
        perror("Couldn't exec subject process");
        abort();
      } else {
        printf("manually run this, then press enter:\n\t%s %s\n", execname, modeBuf);
        getc(stdin);
        exit(0);
      }
    }
    else {
      /* We are in the master process, so wait for the subject process to exit. */
      retval = waitpid(pid, NULL, 0);
      assert(retval != -1);

      /* Fork a new process for the 'Checker'. */
      pid = fork();
      
      if (pid == 0) {
	/* We are in the checker process.  */
	sprintf(modeBuf, "%d", (int) CHECKER_MODE);
        if(!interactive) {
          execl(execname, execname, modeBuf, (char*) 0);
          perror("Couldn't exec checker process");
          abort();
        } else {
          printf("manually run this, then press enter:\n\t%s %s\n", execname, modeBuf);
          getc(stdin);
          exit(0);
        }

      }
      else {
        assert(pid != -1);
	/* We are in the master process, so wait for the checker process to exit. */
	retval = waitpid(pid, &errorFound, 0);
	assert(retval != -1);
	
	/* Exit the loop if an error was found during the checking process. */
	if (errorFound) {
          fail = 1;
          break;
	}
      }
    }

    baseKey += (numThreads * opsPerThread);
  }

  printf("*** Testing Complete: %s ***\n", fail?"FAIL":"PASS"); fflush(stdout);

  fclose(configFile);

  return fail;
}

void
writeGlobalsToSharedBuffer() {
  FILE* iterFile;
  
  iterFile = fopen("iterinfo.txt", "w");
  fprintf(iterFile, "%d %d %d %d %d %d %d %d %d %d\n", 
	  iteration, baseKey, numThreads, opsPerThread, opsPerTransaction, 
	  mallocsBeforeFailure, logWritesBeforeFailure, logWriteAmountDuringFailure, 
	  pageWritesBeforeFailure, pageWriteAmountDuringFailure);
  fclose(iterFile);
}

void
readGlobalsFromSharedBuffer() {
  FILE* iterFile;

  iterFile = fopen("iterinfo.txt", "r");
  fscanf(iterFile, "%d %d %d %d %d %d %d %d %d %d\n", 
	 &iteration, &baseKey, &numThreads, &opsPerThread, &opsPerTransaction, 
	 &mallocsBeforeFailure, &logWritesBeforeFailure, &logWriteAmountDuringFailure, 
	 &pageWritesBeforeFailure, &pageWriteAmountDuringFailure);
  fclose(iterFile);
}


