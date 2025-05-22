/*
 * Copyright 2023 Anton Tananaev (anton@traccar.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.traccar.api.signature;

import org.traccar.model.BaseModel;
import org.traccar.storage.StorageName;

import java.util.Date;

/**
 * Model for storing Certificate Revocation List (CRL) in the database.
 */
@StorageName("tc_certificate_revocation_list")
public class CrlModel extends BaseModel {

    private byte[] crl;

    public byte[] getCrl() {
        return crl;
    }

    public void setCrl(byte[] crl) {
        this.crl = crl;
    }

    private Date lastUpdate;

    public Date getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(Date lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    private Date nextUpdate;

    public Date getNextUpdate() {
        return nextUpdate;
    }

    public void setNextUpdate(Date nextUpdate) {
        this.nextUpdate = nextUpdate;
    }

}