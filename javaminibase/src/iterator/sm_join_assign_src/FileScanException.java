package iterator.sm_join_assign_src;
import chainexception.*;

import java.lang.*;

public class FileScanException extends ChainException {
  public FileScanException(String s){super(null,s);}
  public FileScanException(Exception prev, String s){ super(prev,s);}
}
