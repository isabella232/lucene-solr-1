package org.apache.solr.search.facet;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.Arrays;

public class CountSlotArrAcc extends CountSlotAcc {
  int[] result;

  public CountSlotArrAcc(FacetContext fcontext, int numSlots) {
    super(fcontext);
    result = new int[numSlots];
  }

  @Override
  public void collect(int doc, int slotNum) { // TODO: count arrays can use fewer bytes based on the number of docs in
                                              // the base set (that's the upper bound for single valued) - look at ttf?
    result[slotNum]++;
  }

  @Override
  public int compare(int slotA, int slotB) {
    return Integer.compare(result[slotA], result[slotB]);
  }

  @Override
  public Object getValue(int slotNum) throws IOException {
    return result[slotNum];
  }

  public void incrementCount(int slot, int count) {
    result[slot] += count;
  }

  public int getCount(int slot) {
    return result[slot];
  }

  // internal and expert
  int[] getCountArray() {
    return result;
  }

  @Override
  public void reset() {
    Arrays.fill(result, 0);
  }

  @Override
  public void resize(Resizer resizer) {
    result = resizer.resize(result, 0);
  }
}
