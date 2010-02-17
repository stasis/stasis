#file:Stasis/Web.pm
#----------------------
package StasisWeb::Web;

use threads::shared;
my $thelock :shared;

use strict;
use warnings;
use Apache2::RequestRec ();
use Apache2::RequestIO ();
use Apache2::Const -compile => qw(OK);

sub handler {
    my $r = shift;
    $r->content_type('text/html');
    print "<html><head></head><body><h1>Stasis</h1>" . `pwd`;

    my $output;

    ## XXX This lock acts like a no-op; modapache seems to break perl
    ## threads.
    { lock($thelock);
      my $xid = Stasis::Tbegin();
      warn "a\n";
      my %h;
      tie %h, 'Stasis::Hash', $xid, Stasis::ROOT_RID();

      $h{foo}++;
      
      $output = "$xid $h{foo}\n";
      Stasis::TsoftCommit($xid);
      warn "b\n"; $| = 1;
    }  # Release lock before commit happens.  This is safe, since the output is 
       # deferred, and the commit order safes us from xacts that read uncommitted
       # data.
    Stasis::TforceCommits();
    print "$output</body></html>\n";
  
    return Apache2::Const::OK;
}
1;
