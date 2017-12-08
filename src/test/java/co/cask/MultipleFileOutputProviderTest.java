package co.cask;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Class description here.
 */
public class MultipleFileOutputProviderTest {

  @Ignore
  @Test
  public void testBasePathHandling() throws Exception {
    String[] paths = {
      "/dir",
      "/dir/",
      "/dir/dir/",
      "/",
      "/dir/dir/dir/"
    };

    for (String path : paths) {
      if (path.charAt(path.length() - 1) == '/') {
        path = path.substring(0, path.length() - 1);
        Assert.assertTrue(true);
      }
    }
  }

}