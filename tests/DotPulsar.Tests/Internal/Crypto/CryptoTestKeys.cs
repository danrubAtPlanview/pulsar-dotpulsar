/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace DotPulsar.Tests.Internal.Crypto;

using System.Text;

// The test keys of apache/pulsar@f3846f4 (Apache License, Version 2.0)
public static class CryptoTestKeys
{
    // pulsar-broker/src/test/resources/certificate/public-key.client-rsa.pem
    public const string ClientRsaPublic = """
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtKWwgqdnTYrOCv+j1MkT
WfSH0wCsHZZca9wAW3qP4uuhlBvnb10JcFf5ZjzP9BSXK+tHmI8uoN368vEv6yhU
RHM4yuXqzCxzuAwkQSo39rzX8PGC7qdjCN7LDJ3MnqiBIrUsSaEP1wrNsB1kI+o9
ER1e5O/uEPAotP933hHQ0J2hMEekHqL7sBlJ98h6NmsicEaUkardk0TOXrlkjC+c
Md8ZbGScPqI9M38bmn3OLxFTn1vthpvnXLvCmG4M+6xtYtD+npcVPZw1i1R90fMs
7ppZnRbv8Hc/DFdOKVQIgam6CDdnNKgW7c7IBMrP0AEm37HTu0LSOjP2OHXlvvlQ
GQIDAQAB
-----END PUBLIC KEY-----
""";

    // pulsar-broker/src/test/resources/certificate/private-key.client-rsa.pem
    public const string ClientRsaPrivate = """
-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAtKWwgqdnTYrOCv+j1MkTWfSH0wCsHZZca9wAW3qP4uuhlBvn
b10JcFf5ZjzP9BSXK+tHmI8uoN368vEv6yhURHM4yuXqzCxzuAwkQSo39rzX8PGC
7qdjCN7LDJ3MnqiBIrUsSaEP1wrNsB1kI+o9ER1e5O/uEPAotP933hHQ0J2hMEek
HqL7sBlJ98h6NmsicEaUkardk0TOXrlkjC+cMd8ZbGScPqI9M38bmn3OLxFTn1vt
hpvnXLvCmG4M+6xtYtD+npcVPZw1i1R90fMs7ppZnRbv8Hc/DFdOKVQIgam6CDdn
NKgW7c7IBMrP0AEm37HTu0LSOjP2OHXlvvlQGQIDAQABAoIBAAaJFAi2C7u3cNrf
AstY9vVDLoLIvHFZlkBktjKZDYmVIsRb+hSCViwVUrWLL67R6+Iv4eg4DeTOAx00
8pncXKgZTw2wIb1/QjR/Y/RjlaC8lkdmRWli7udMQCZVsyhuSjW6Pj7vr8YE4woj
FhNijxEGcf9wWrmMJrzdnTWQiXByo+eTvUQ9BPgPGrRjsMZmTkLyAVJff2DfxO5b
IWFDYDJcyYAMCIMQu7vys/I50ou6ilb1CO6QM6Z7KpPeOoVFPwtzbh8cf9xM8UNS
j6J/JmdWhgI34GS3NA68xTQ6PV7zjnhCc+iccm3JKyzGXwaApAZ+Eoce/9j4WKmu
5B4ziR0CgYEA3l/9OHbl1zmyV+rRxWOIj/i2rTvHzwBnbnPJyuemL5VMFdpGodQ3
vwHvyQmcECRVRxmXojQ4QuPPHs3qp6wEEFPCWxChLSTxlUc85SOFHWU2O99jV7zI
7+JOpDK/Mstsx9nHgXduJF+glTFtA3LH8Oqylzu2aFPsprwKuZf94Q8CgYEAz/Zx
akEG+PEMtP5YS28cX5XfjsIX/V26Fs6/sH16QjUIEddE5T4fCuokxCjSiwUcWhml
pHEJ5S5xp3VYRfISW3jRW3qstIH1tpZipB6+S0zTuJmLJbA3IiWEg2rtMt7X1uJv
A/bYOqe0hOPTuXuZdtVZ0nMTKk7GG8O6VkBI7FcCgYEAkDfCmscJgs7JahlBWHmX
zH9pwem+SPKjIc/4NB6N+dgikx2Pp05hpP/VihUwYIufvs/LNogVYNQrtHepUnrN
2+TmbHbZgNSv1Ldxt82UfB7y0FutKu6lhmXHyNecho3Fi8sih0V0aiSWmYuHfrAH
GaiskEZKo1iiZvQXJIx9O2MCgYATBf0r9hTYMtyxtc6H3/sdd01C9thQ8gDy0yjP
0Tqc0dMSJroDqmIWkoKYew9/bhFA4LW5TCnWkCAPbHmNtG4fdfbYwmkH/hdnA2y0
jKdlpfp8GXeUFAGHGx17FA3sqFvgKUh0eWEgRHUL7vdQMVFBgJS93o7zQM94fLgP
6cOB8wKBgFcGV4GjI2Ww9cillaC554MvoSjf8B/+04kXzDOh8iYIIzO9EUil1jjK
Jvxp4hnLzTKWbux3MEWqurLkYas6GpKBjw+iNOCar6YdqWGVqM3RUx7PTUaZwkKx
UdP63IfY7iZCIT/QbyHQvIUe2MaiVnH+ulxdkK6Y5e7gxcbckIH4
-----END RSA PRIVATE KEY-----
""";

    // pulsar-broker/src/test/resources/certificate/public-key.client-rsa-pkcs8.pem
    public const string ClientRsaPkcs8Public = """
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAt8SdLqwx491+J1Je4/KL
8xzFCu+IZkSMvp9Vbj21HH2h2F8epaXmwLKxc4L6rzVMsFxlhF2fs5T6Qw3zLqWd
CltqxgJTkIRliPN/OD7QSDYtOo2NFmtQKXNhCeN6z/ZR0f15GUR2jw0TURXq97sJ
W9YYccpYjUR9Rma368l9xVtf8/XIRvqLnPQLz/vzSWDtlJ/JJT8bLMBAzpK04GW3
QF8ANI2Gn1YiL/nmu8bHrPmIDmmenL/CaQ9awRdE8DtVNmp0Ecme41Z5WXkDBDOE
4pgpt4unoB6VJ28NAbEr6FCjq5KV57qwk6X8dObMt8hbphQpvMrXYrRwed5gRLXX
dwIDAQAB
-----END PUBLIC KEY-----
""";

    // pulsar-broker/src/test/resources/certificate/private-key.client-rsa-pkcs8.pem
    public const string ClientRsaPkcs8Private = """
-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC3xJ0urDHj3X4n
Ul7j8ovzHMUK74hmRIy+n1VuPbUcfaHYXx6lpebAsrFzgvqvNUywXGWEXZ+zlPpD
DfMupZ0KW2rGAlOQhGWI8384PtBINi06jY0Wa1Apc2EJ43rP9lHR/XkZRHaPDRNR
Fer3uwlb1hhxyliNRH1GZrfryX3FW1/z9chG+ouc9AvP+/NJYO2Un8klPxsswEDO
krTgZbdAXwA0jYafViIv+ea7xses+YgOaZ6cv8JpD1rBF0TwO1U2anQRyZ7jVnlZ
eQMEM4TimCm3i6egHpUnbw0BsSvoUKOrkpXnurCTpfx05sy3yFumFCm8ytditHB5
3mBEtdd3AgMBAAECggEAPJJEYulkCmDr34ches3r/iaJ9hhM47SNlZlMHemeBDWO
vpRftCbSP8rGfbX67fxJ+kmCNpZreNROhwKWmNYCxLKsJ3cF1zkkhHcKBF6D5YFh
+A5HJlAZ3IfIwOsnhFuCzY3YOBFeaLldGBl62MvgAlK3vO3+sRWVmzppPgEsgPuU
KEGFB4EX2JRY001rQcfQ4Oh2VcR5st13/JAXw6aMfJAtYAHAt5rpiYQZyH/t25+a
aqVTp5Q/oyx/+hNajU0gasvAaRtUGi1HipUkcZ80RTTMFKsMn/gM1kyPJTQMejcC
o74/xhyGdGQ1hLFijXLMn8JclifIOUDBrVySZagJ1QKBgQD8kwvzhlehBZ2dQkjJ
T+S2suuuE6UPJDSJiq+7gjy4F11p+NAZv2QhLzIGRlX0WmR0BU0Nt1X50uWIUnwM
NJuz5JWIWIxcyBoCRD3BFHzq1yGBbP03iHyiYW864+VZy2INJmz0Oww4kC55kzdH
aB9IVQRjCyC5po38zwHdKJAVXQKBgQC6QqrgATeVWDwKbabbIJY5uqsK8VVphCTi
4ezRhA5XldaTA3zODg5fo+oa3Yge0CHx7moM5odCqjm8dBPdE0D9vaZ/dEsIVqcZ
H6oEV+xuLePTXsFDcX80EwtIB/DJs5sqFIcqUYyrPBPHrY9dsfXnohBGlYysd+T5
yLwgSxoe4wKBgQCmjBQyZeC8piZTf1TdolgPfRXZuMx7moUmORQ+EdgWEBoh+dbH
U6QkGanj51dghIZ+1swDgmQoE0AVz0ZLChmt2sobmUAdIjm8SuFpwyfH90n5vTOP
/ekpbhRZgdNHjcw/HZp0xcb+OVzcAA5QgME4Ag+de8X9hW1VSe9RtSClZQKBgHPO
98eHejPVRJ+HRfOA82a5dZe8LcsJapXWuxQZ84DpnRyTUoobmzxEAk7p5rSh3Dk8
b5vNydPdW0CdO6x9SC98N4WPnQd2OTcZxPAmas6mobzOcYOpkdmi7aS/qyr+DT/A
u7nIMppVJEdG1pj/CVIYJd59JZYmhav471900qzZAoGBAKP5f1vSQtcN9PNMUidy
c3rNF3jVnGmV6LXIeMk0dwlMMANT2X3CzXTUU9y80AXThHEltJtdHD5xjJjRTODv
rWxAAZRnLDCfv/4meD+3YRWXw8oy6hG81vz4v81ypE5jD37/kwTIfbpSVGxo699J
c3KEpMHolMtHSTl9WWz3SJwM
-----END PRIVATE KEY-----
""";

    // pulsar-broker/src/test/resources/certificate/public-key.client-mismatch-rsa.pem (the same key as public-key.client-rsa.pem)
    public const string ClientMismatchRsaPublic = """
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtKWwgqdnTYrOCv+j1MkT
WfSH0wCsHZZca9wAW3qP4uuhlBvnb10JcFf5ZjzP9BSXK+tHmI8uoN368vEv6yhU
RHM4yuXqzCxzuAwkQSo39rzX8PGC7qdjCN7LDJ3MnqiBIrUsSaEP1wrNsB1kI+o9
ER1e5O/uEPAotP933hHQ0J2hMEekHqL7sBlJ98h6NmsicEaUkardk0TOXrlkjC+c
Md8ZbGScPqI9M38bmn3OLxFTn1vthpvnXLvCmG4M+6xtYtD+npcVPZw1i1R90fMs
7ppZnRbv8Hc/DFdOKVQIgam6CDdnNKgW7c7IBMrP0AEm37HTu0LSOjP2OHXlvvlQ
GQIDAQAB
-----END PUBLIC KEY-----
""";

    // pulsar-broker/src/test/resources/certificate/private-key.client-mismatch-rsa.pem (despite the name, this is a secp521r1 key with explicit parameters)
    public const string ClientMismatchRsaPrivate = """
-----BEGIN EC PARAMETERS-----
MIIBwgIBATBNBgcqhkjOPQEBAkIB////////////////////////////////////
//////////////////////////////////////////////////8wgZ4EQgH/////
////////////////////////////////////////////////////////////////
/////////////////ARBUZU+uWGOHJofkpohoLaFQO6i2nJbmbMV87i0iZGO8Qnh
Vhk5Uex+k3sWUsC9O7G/BzVz34g9LDTx70Uf1GtQPwADFQDQnogAKRy4U5bMZxc5
MoSqoNpkugSBhQQAxoWOBrcEBOnNnj7LZiOVtEKcZIE5BT+1Ifgor2BrTT26oUte
d+/nWSj+HcEnov+o3jNIs8GFakKb+X5+McLlvWYBGDkpaniaO8AEXIpftCx9G9mY
9URJV5tEaBevvRcnPmYsl+5ymV70JkDFULkBP60HYTU8cIaicsJAiL6Udp/RZlAC
QgH///////////////////////////////////////////pRhoeDvy+Wa3/MAUj3
CaXQO7XJuImcR667b7cekThkCQIBAQ==
-----END EC PARAMETERS-----
-----BEGIN EC PRIVATE KEY-----
MIICnQIBAQRCAeNLEp1HefZ1nMl5vvgFMsJCd5ieCWqPT7TXbQkn27A8WkyAGTYC
GtolyPokOgSjbJh+ofBt/MgvE/nMrqzmkZVtoIIBxjCCAcICAQEwTQYHKoZIzj0B
AQJCAf//////////////////////////////////////////////////////////
////////////////////////////MIGeBEIB////////////////////////////
//////////////////////////////////////////////////////////wEQVGV
PrlhjhyaH5KaIaC2hUDuotpyW5mzFfO4tImRjvEJ4VYZOVHsfpN7FlLAvTuxvwc1
c9+IPSw08e9FH9RrUD8AAxUA0J6IACkcuFOWzGcXOTKEqqDaZLoEgYUEAMaFjga3
BATpzZ4+y2YjlbRCnGSBOQU/tSH4KK9ga009uqFLXnfv51ko/h3BJ6L/qN4zSLPB
hWpCm/l+fjHC5b1mARg5KWp4mjvABFyKX7QsfRvZmPVESVebRGgXr70XJz5mLJfu
cple9CZAxVC5AT+tB2E1PHCGonLCQIi+lHaf0WZQAkIB////////////////////
///////////////////////6UYaHg78vlmt/zAFI9wml0Du1ybiJnEeuu2+3HpE4
ZAkCAQGhgYkDgYYABAFhUHeaHfIWre/pPmv2a2l891co79dFpg6ixPRg+Y5qe0C7
src//LT/ZR5rgj8ne+YcaIlwyQRl5OYEd25n799IcgHIBTGyaLB6Td5mW/oWT/Fz
soufOnUJ7O/kDHjIQ15sczk3rDhe8/mB9zPjKlKTuAl5jBEt6E3yiB44Dtng02xD
uQ==
-----END EC PRIVATE KEY-----
""";

    // pulsar-broker/src/test/resources/certificate/public-key.client-ecdsa.pem
    public const string ClientEcdsaPublic = """
-----BEGIN PUBLIC KEY-----
MIHKMIGjBgcqhkjOPQIBMIGXAgEBMBwGByqGSM49AQECEQD////9////////////
////MDsEEP////3///////////////wEEOh1ecEQefQ92CSZPCzuXtMDFQAADg1N
aW5naHVhUXUMwDpEc9A2eQQhBBYf91KLiZstDChgfKUsW4bPWsg5W6/rE8AtopLd
7XqDAhEA/////gAAAAB1ow0bkDihFQIBAQMiAATrKj6RPHGPNKcZKIOccN4tgENM
n1dzKjLrMZTkJ4oAaQ==
-----END PUBLIC KEY-----
""";

    // pulsar-broker/src/test/resources/certificate/private-key.client-ecdsa.pem
    public const string ClientEcdsaPrivate = """
-----BEGIN EC PARAMETERS-----
MIGXAgEBMBwGByqGSM49AQECEQD////9////////////////MDsEEP////3/////
//////////wEEOh1ecEQefQ92CSZPCzuXtMDFQAADg1NaW5naHVhUXUMwDpEc9A2
eQQhBBYf91KLiZstDChgfKUsW4bPWsg5W6/rE8AtopLd7XqDAhEA/////gAAAAB1
ow0bkDihFQIBAQ==
-----END EC PARAMETERS-----
-----BEGIN EC PRIVATE KEY-----
MIHYAgEBBBDeu9hc8kOvL3pl+LYSjLq9oIGaMIGXAgEBMBwGByqGSM49AQECEQD/
///9////////////////MDsEEP////3///////////////wEEOh1ecEQefQ92CSZ
PCzuXtMDFQAADg1NaW5naHVhUXUMwDpEc9A2eQQhBBYf91KLiZstDChgfKUsW4bP
Wsg5W6/rE8AtopLd7XqDAhEA/////gAAAAB1ow0bkDihFQIBAaEkAyIABOsqPpE8
cY80pxkog5xw3i2AQ0yfV3MqMusxlOQnigBp
-----END EC PRIVATE KEY-----
""";

    // pulsar-client/src/test/resources/crypto_rsa_public.key
    public const string CryptoRsaPublic = """
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAzzTTzsSsZFYlWybZrMNw
aDjgqinINo5yNkHuRBPg2r56BEaHoU5y+ccDhyxBGCKPZkTcQasvYgWJ3sHRKAlN
dZNG8Gt3k2SrfDrrtj1KL3G6NWRA8TqyRkxxl5vpAMc69UjX9HPwSzlmrC7ZXm1e
SwYTV7+7qs/69JLBnrMJcsl+IyXUahhRn8w2Fks9JWriNKi1PSgCPjMjgKBF7yaE
ATJ0GMSMc8FvXWwFJsWBWQkUwgqlExR1MDiVVBtw9QtJB29INi0NDs2Peb61Dt49
NZlN/klJCXIUtBSIqg9o+iRKVwXLHnIM0WHVnmRn2MK0nf03/DwCIVnb5elToZ72
8wIDAQAB
-----END PUBLIC KEY-----
""";

    // pulsar-client/src/test/resources/crypto_rsa_private.key
    public const string CryptoRsaPrivate = """
-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAzzTTzsSsZFYlWybZrMNwaDjgqinINo5yNkHuRBPg2r56BEaH
oU5y+ccDhyxBGCKPZkTcQasvYgWJ3sHRKAlNdZNG8Gt3k2SrfDrrtj1KL3G6NWRA
8TqyRkxxl5vpAMc69UjX9HPwSzlmrC7ZXm1eSwYTV7+7qs/69JLBnrMJcsl+IyXU
ahhRn8w2Fks9JWriNKi1PSgCPjMjgKBF7yaEATJ0GMSMc8FvXWwFJsWBWQkUwgql
ExR1MDiVVBtw9QtJB29INi0NDs2Peb61Dt49NZlN/klJCXIUtBSIqg9o+iRKVwXL
HnIM0WHVnmRn2MK0nf03/DwCIVnb5elToZ728wIDAQABAoIBAHWFvfiRnkGOhsOL
ZnzRoMjMMcjxx8gBxQ+3F1/vcmI/FM+l/TllWFsJIJwjYoxA1dqohdCNOmO7RnZc
sbemhxN/xAWKvpiPyZ+9f4Gug4wjUf0Ebr0jkIfExcy6tk4lse/7L9lLhOf1l6Fj
NRCUsZ2VxZTIf7WjHvBm6ICNhXdfgc/TOX/H4BBMxyQkkmvS7yQHPmnekVpCjwXi
RgdPOpBSHUAsuLc3cdO7DzSlXBy+R3UB5bC97efSTwxmMdcGUNQh17Cuw+ou2OLJ
l/WyMBJgKP0zp8MI2YCP0toE1VV0FWisiNUdyw2mmdsKBPCtZW4Jf/avRLjCpy83
gyeHi4kCgYEA7fac8u/WoUcRxfwbexxVNIb5jpVgQ22XXUvcOBA3145HFH4kD9Qs
OlOM48iEX1Gq+FOb+TkVa3yeEbyEHVmNxm7ZqDElGlPnHHgWJfVo4ltemkNQ8cQI
CiDhUH7D9iGd4TrLq+u8JY/oy0dpJyaJ/GsNPwjVzMiA9kDuI2KKRpcCgYEA3ulp
sZy82EibT+YYNshfAyzPcQc+CVdcpS3yEUMcMrrGT2IHbAhldzAWvni7PF1CvYoB
eoPSGiEGMAwAfaUI4psZvATZTf+GWKizlH820lw4tY2NW2tYFwDcZ6EPKdq9ZCOz
Lfyw2NhLqi2FpFyApcXlA92UUI0FcXCCtAKJ2gUCgYEAsI5merVKiNTDMiNQfHJU
Ian3pTvddYntYXJ0jUAzPoK46FKDDR9+RTRSd3sCA/sDIEZglnQtGVgXq880MtaM
I2uBoJH+Fl+kPPI4dKd1z2S9dzV07DxnPqSQp/m2CXt9uWu3S/KW4UO6FIECWuL0
RE1lQZybjNpDHPKl/akSMTcCgYBqC80WjCRjGJeauTJHzac10XmWogVnWEJg6qzA
fZbKo4R4e4BgatYqj5wiXTlmDFAV77OoX1Hy0EcrUGpjW8IQXA0wH3Zp3uhBAXD9
ck/YX7sy0/atyTGNMAGq4zpdhQyYuUsi05Ymcy/789AiU0d4ld7PqfhHIe+2+fmU
PajrKQKBgQCjpj0Y+E/Hi0SJUKNVPl3u+SN1lLcR1/gMjJbsYFGua1MsLXBNXTSl
QiYHiadT7BhPEkFdW7u+bFws2admW19BoUb+wgtZT/t7nTyoS4Lag4vyazNPZzdQ
x6XPrwZimd0XDDitGLjcLf991EFsXSqPznDDfXtJ31+oU6ObnHSItA==
-----END RSA PRIVATE KEY-----
""";

    // pulsar-client/src/test/resources/crypto_ecdsa_public.key
    public const string CryptoEcdsaPublic = """
-----BEGIN PUBLIC KEY-----
MIICXDCCAc8GByqGSM49AgEwggHCAgEBME0GByqGSM49AQECQgH/////////////
////////////////////////////////////////////////////////////////
/////////zCBngRCAf//////////////////////////////////////////////
///////////////////////////////////////8BEFRlT65YY4cmh+SmiGgtoVA
7qLacluZsxXzuLSJkY7xCeFWGTlR7H6TexZSwL07sb8HNXPfiD0sNPHvRR/Ua1A/
AAMVANCeiAApHLhTlsxnFzkyhKqg2mS6BIGFBADGhY4GtwQE6c2ePstmI5W0Qpxk
gTkFP7Uh+CivYGtNPbqhS1537+dZKP4dwSei/6jeM0izwYVqQpv5fn4xwuW9ZgEY
OSlqeJo7wARcil+0LH0b2Zj1RElXm0RoF6+9Fyc+ZiyX7nKZXvQmQMVQuQE/rQdh
NTxwhqJywkCIvpR2n9FmUAJCAf//////////////////////////////////////
////+lGGh4O/L5Zrf8wBSPcJpdA7tcm4iZxHrrtvtx6ROGQJAgEBA4GGAAQBalBI
5bNN20KMCUFEp04qs073VxNujTfPdUh6A8dsKn0YNjko8yNW0HAU9hgEX7rdP7VQ
28d4F/KpmjxGPbp93boB0hIF9tPci4R7l3yCSmZumIp3S+kccC+lnar/W7B1Iu3U
l0wSHOtAtGuoMc4V9CPT+IPjoPIa7FtB5pptcyUPMYE=
-----END PUBLIC KEY-----
""";

    // pulsar-client/src/test/resources/crypto_ecdsa_private.key
    public const string CryptoEcdsaPrivate = """
-----BEGIN EC PARAMETERS-----
MIIBwgIBATBNBgcqhkjOPQEBAkIB////////////////////////////////////
//////////////////////////////////////////////////8wgZ4EQgH/////
////////////////////////////////////////////////////////////////
/////////////////ARBUZU+uWGOHJofkpohoLaFQO6i2nJbmbMV87i0iZGO8Qnh
Vhk5Uex+k3sWUsC9O7G/BzVz34g9LDTx70Uf1GtQPwADFQDQnogAKRy4U5bMZxc5
MoSqoNpkugSBhQQAxoWOBrcEBOnNnj7LZiOVtEKcZIE5BT+1Ifgor2BrTT26oUte
d+/nWSj+HcEnov+o3jNIs8GFakKb+X5+McLlvWYBGDkpaniaO8AEXIpftCx9G9mY
9URJV5tEaBevvRcnPmYsl+5ymV70JkDFULkBP60HYTU8cIaicsJAiL6Udp/RZlAC
QgH///////////////////////////////////////////pRhoeDvy+Wa3/MAUj3
CaXQO7XJuImcR667b7cekThkCQIBAQ==
-----END EC PARAMETERS-----
-----BEGIN EC PRIVATE KEY-----
MIICnQIBAQRCAPvNzZPrKUZao8oLRet7MUTfa9TUEJYw2C4TyMSP54YWzyoQsGJJ
FQVGsJJWBvmEDTfU5zU6d+vvbLVWy7R0aMDXoIIBxjCCAcICAQEwTQYHKoZIzj0B
AQJCAf//////////////////////////////////////////////////////////
////////////////////////////MIGeBEIB////////////////////////////
//////////////////////////////////////////////////////////wEQVGV
PrlhjhyaH5KaIaC2hUDuotpyW5mzFfO4tImRjvEJ4VYZOVHsfpN7FlLAvTuxvwc1
c9+IPSw08e9FH9RrUD8AAxUA0J6IACkcuFOWzGcXOTKEqqDaZLoEgYUEAMaFjga3
BATpzZ4+y2YjlbRCnGSBOQU/tSH4KK9ga009uqFLXnfv51ko/h3BJ6L/qN4zSLPB
hWpCm/l+fjHC5b1mARg5KWp4mjvABFyKX7QsfRvZmPVESVebRGgXr70XJz5mLJfu
cple9CZAxVC5AT+tB2E1PHCGonLCQIi+lHaf0WZQAkIB////////////////////
///////////////////////6UYaHg78vlmt/zAFI9wml0Du1ybiJnEeuu2+3HpE4
ZAkCAQGhgYkDgYYABAFqUEjls03bQowJQUSnTiqzTvdXE26NN891SHoDx2wqfRg2
OSjzI1bQcBT2GARfut0/tVDbx3gX8qmaPEY9un3dugHSEgX209yLhHuXfIJKZm6Y
indL6RxwL6Wdqv9bsHUi7dSXTBIc60C0a6gxzhX0I9P4g+Og8hrsW0Hmmm1zJQ8x
gQ==
-----END EC PRIVATE KEY-----
""";

    public static byte[] Public(string keySet)
        => Encoding.UTF8.GetBytes(keySet switch
        {
            "ClientRsa" => ClientRsaPublic,
            "ClientRsaPkcs8" => ClientRsaPkcs8Public,
            "CryptoRsa" => CryptoRsaPublic,
            "ClientEcdsa" => ClientEcdsaPublic,
            "CryptoEcdsa" => CryptoEcdsaPublic,
            _ => throw new ArgumentOutOfRangeException(nameof(keySet), keySet, "Unknown key set")
        });

    public static byte[] Private(string keySet)
        => Encoding.UTF8.GetBytes(keySet switch
        {
            "ClientRsa" => ClientRsaPrivate,
            "ClientRsaPkcs8" => ClientRsaPkcs8Private,
            "CryptoRsa" => CryptoRsaPrivate,
            "ClientEcdsa" => ClientEcdsaPrivate,
            "CryptoEcdsa" => CryptoEcdsaPrivate,
            _ => throw new ArgumentOutOfRangeException(nameof(keySet), keySet, "Unknown key set")
        });
}
