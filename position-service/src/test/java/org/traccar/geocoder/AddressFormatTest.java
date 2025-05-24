/*
 * Copyright 2015 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.geocoder;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AddressFormatTest {

    private void test(Address address, String format, String expected) {
        assertEquals(expected, new AddressFormat(format).format(address));
    }

    @Test
    public void testFormat() {

        Address address = new Address();
        address.setCountry("NZ");
        address.setSettlement("Auckland");
        address.setStreet("Queen St");
        address.setHouse("1A");

        test(address, "%h %r %t %d %s %c %p", "1A Queen St Auckland NZ");
        test(address, "%h %r %t", "1A Queen St Auckland");
        test(address, "%h %r, %t", "1A Queen St, Auckland");
        test(address, "%h %r, %t %p", "1A Queen St, Auckland");
        test(address, "%t %d %c", "Auckland NZ");
        test(address, "%t, %d, %c", "Auckland, NZ");
        test(address, "%d %c", "NZ");
        test(address, "%d, %c", "NZ");
        test(address, "%p", "");
    }

}