package iterator.sm_join_assign_src;
import chainexception.*;

import java.lang.*;

public class JoinsException extends ChainException {
  public JoinsException(String s){super(null,s);}
  public JoinsException(Exception prev, String s){ super(prev,s);}
}
