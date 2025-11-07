// Copyright (C) 2025 GerritForge, Inc.
//
// Licensed under the BSL 1.1 (the "License");
// you may not use this file except in compliance with the License.
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.gerritforge.gerrit.plugins.kafka;

import com.google.gerrit.metrics.Description;
import com.google.gerrit.metrics.Field;
import com.google.gerrit.server.logging.PluginMetadata;

public abstract class KafkaEventsMetrics {

  public Field<String> stringField(String metadataKey, String description) {
    return Field.ofString(
            metadataKey,
            (metadataBuilder, fieldValue) ->
                metadataBuilder.addPluginMetadata(PluginMetadata.create(metadataKey, fieldValue)))
        .description(description)
        .build();
  }

  public Description rateDescription(String unit, String description) {
    return new Description(description).setRate().setUnit(unit);
  }
}
