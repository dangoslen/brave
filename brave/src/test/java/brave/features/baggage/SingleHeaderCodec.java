/*
 * Copyright 2013-2020 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package brave.features.baggage;

import brave.baggage.BaggageField;
import brave.internal.baggage.BaggageCodec;
import brave.internal.baggage.ExtraBaggageFields;
import brave.propagation.TraceContext;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This is a non-complete codec for the w3c (soon to be renamed to "baggage") header.
 *
 * <p>See https://github.com/w3c/correlation-context/blob/master/correlation_context/HTTP_HEADER_FORMAT.md
 */
final class SingleHeaderCodec implements BaggageCodec {
  static final SingleHeaderCodec INSTANCE = new SingleHeaderCodec();

  static BaggageCodec get() {
    return INSTANCE;
  }

  final String keyName = "baggage";
  final List<String> keyNames = Collections.singletonList(keyName);

  @Override public List<String> extractKeyNames() {
    return keyNames;
  }

  @Override public List<String> injectKeyNames() {
    return keyNames;
  }

  @Override public boolean decode(ExtraBaggageFields extra, Object request, String value) {
    if (!extra.isDynamic()) {
      // This will drop values not in the whitelist!
    }
    boolean decoded = false;
    for (String entry : value.split(",")) {
      String[] keyValue = entry.split("=", 2);
      if (BaggageField.create(keyValue[0]).updateValue(keyValue[1])) decoded = true;
    }
    return decoded;
  }

  @Override
  public String encode(Map<BaggageField, String> values, TraceContext context, Object request) {
    StringBuilder result = new StringBuilder();
    for (Entry<BaggageField, String> entry : values.entrySet()) {
      if (entry.getValue() != null) {
        if (result.length() > 0) result.append(',');
        result.append(entry.getKey().name()).append('=').append(entry.getValue());
      }
    }
    return result.length() == 0 ? null : result.toString();
  }
}
