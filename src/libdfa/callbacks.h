/*---
This software is copyrighted by the Regents of the University of
California, and other parties. The following terms apply to all files
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
#include <libdfa/libdfa.h>
/**
   A covienence function that responds to an incoming message one
   time.  (Useful for sending ACK's, and then freeing a machine.)
   @param ns               The network setup that will be used to send the message.
   @param type             The 'local state' that the message should respond with
   @param incoming_message The message that we're responding to.  This can be a broadcast addess such as bc:1.
   @param from             The network address of that we're responding to.
   @see NetworkSetup
*/

state_name respond_once(NetworkSetup * ns, state_name type, Message * incoming_message, char * from);
/**
   A convienence function that keeps track of responses from multiple
   machines.  For example, this is used by two phase commit to count
   PREPARED messages.

   @param broadcast_list An array of strings containing the addresses
   of the remote machines.

   @param host_count The number of strings in broadcast_list

   @param bitSet An array of host_count characters, each initialized
   to zero.  (as in (unsigned char)0, not '0') The application should
   manage this memory, and only initializes it when its time to start
   a fresh tally.

   @param from The address of the remote machine we just recieved a
   message from.

   @return 1 if all hosts have responded, 0 otherwise.
*/
state_name tally(char ** broadcast_list, int host_count, char * bitSet, char * from);

