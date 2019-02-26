package iterator.sm_join_assign_src;

import java.lang.*;
import chainexception.*;

public class SortException extends ChainException 
{
  public SortException(String s) {super(null,s);}
  public SortException(Exception e, String s) {super(e,s);}
}
