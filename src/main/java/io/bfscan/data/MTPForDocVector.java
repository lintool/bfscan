/*
 * ClueWeb Tools: Hadoop tools for manipulating ClueWeb collections
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package io.bfscan.data;

import me.lemire.integercompression.Composition;
import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.IntegerCODEC;
import me.lemire.integercompression.VariableByte;
import tl.lin.data.array.IntArrayWritable;

public class MTPForDocVector {
  private final VariableByte VB = new VariableByte();
  private final IntegerCODEC codec =  new Composition(new FastPFOR(), new VariableByte());
  private int[] termids;

  public MTPForDocVector() {}
  
  
  public int[] getTermIds() {
    return termids;
  }

  public int getLength() {
    return termids.length;
  }

  public void fromIntArrayWritable(IntArrayWritable in, MTPForDocVector doc) {
    try {
      int[] compressed = in.getArray();
      IntWrapper inPos = new IntWrapper(1);
      IntWrapper outPos = new IntWrapper(0);
      doc.termids = new int[compressed[0]];
      if (doc.termids.length == 0) {
        return;
      }

      if (doc.termids.length < 128) {
        VB.uncompress(compressed, inPos, in.size()-1, doc.termids, outPos);
        return;
      }

      codec.uncompress(compressed, inPos, in.size()-1, doc.termids, outPos);

    } catch (Exception e) {
      e.printStackTrace();
      doc.termids = new int[0];
    }
  }

  public void toIntArrayWritable(IntArrayWritable ints, int[] termids, int length) {
    // Remember, the number of terms to serialize is length; the array might be longer.
    try {
      if (termids == null) {
        termids = new int[] {};
        length = 0;
      }
      IntWrapper inPos = new IntWrapper(0);
      IntWrapper outPos = new IntWrapper(1);

      int[] out = new int[length + 1];
      out[0] = length;

      if (length < 128) {
        VB.compress(termids, inPos, length, out, outPos);
        ints.setArray(out, outPos.get());
        return;
      }
      
      // encode using combination of PFor and VByte
      codec.compress(termids, inPos, length, out, outPos);
      ints.setArray(out, outPos.get());
    } catch (Exception e) {
      e.printStackTrace();
      ints.setArray(new int[] {}, 0);
    }
  }
}
