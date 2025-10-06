## <!-- 1. Aggregated = Aggregated values of various payment categories as shown under Categories section-->
<!-- Dataset structure of aggregated transactions -->
Transaction data broken down by type of payment -
{
    "success": true, //Ignore. For internal use only
    "code": "SUCCESS", //Ignore. For internal use only
    "data": {
        "from": 1514745000000, //Data duration
        "to": 1522175400000,
        "transactionData": [
            {
                "name": "Recharge & bill payments", //Type of payment category
                "paymentInstruments": [
                    {
                        "type": "TOTAL",
                        "count": 72550406, //Total number of transactions for the above payment category
                        "amount": 1.4472713558652578E10 //Total value
                    }
                ]
            },

            ...,

            ...,

            {
                "name": "Others",
                "paymentInstruments": [
                    {
                        "type": "TOTAL",
                        "count": 5761576,
                        "amount": 4.643217301269438E9
                    }
                ]
            }
        ]
    },
    "responseTimestamp": 1630346628866 //Ignore. For internal use only.
}

<!-- SQL table structure of aggregated transactions -->
use phonepe_insights;

 CREATE TABLE IF NOT EXISTS aggregated_transactions_stats (
   id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
   state               VARCHAR(64)      NOT NULL,
   year                INT              NOT NULL,
   quarter             TINYINT          NOT NULL CHECK (quarter BETWEEN 1 AND 4),
   transaction_type    VARCHAR(64)      NOT NULL,
   transaction_count   BIGINT UNSIGNED  NOT NULL,
   transaction_amount  DECIMAL(20,2)    NOT NULL,
   PRIMARY KEY (id)

 );
<!-- ------------------------------------------------------------------------------------------------------------------------------------------- -->
<!-- Dataset structure of aggregated Users data -->
Users data broken down by devices 

{
    "success": true, //Ignore. For internal use only.
    "code": "SUCCESS", //Ignore. For internal use only.
    "data": {
        "aggregated": {
            "registeredUsers": 284985430, //Total number of registered users for the selected quarter.
            "appOpens": 8635508502 //Number of app opens by users for the selected quarter
        },
        "usersByDevice": [ //Users by individual device
            {
                "brand": "Xiaomi", //Brand name of the device
                "count": 71553154, //Number of registered users by this brand.
                "percentage": 0.2510765339828075 //Percentage of share of current device type compared to all devices.
            },

            ...,

            ...,

            {
                "brand": "Others", //All unrecognized device types grouped here.
                "count": 23564639, //Number of registered users by all unrecognized device types.
                "percentage": 0.08268717105993804 //Percentage of share of all unrecognized device types compared to overall devices that users are registered with.
            }
        ]
    },
    "responseTimestamp": 1630346630074 //Ignore. For internal use only.
}

<!-- SQL table structure of aggregated User data -->
Total number of registered users for the selected quarter of particular state
use phonepe_insights;

 CREATE TABLE aggregated_user_state_totals (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  state        VARCHAR(64)     NOT NULL,   -- pulse key e.g. 'maharashtra'
  year         INT             NOT NULL,
  quarter      TINYINT         NOT NULL,   -- 1..4
  total_users  BIGINT UNSIGNED NOT NULL,   -- aggregated.registeredUsers
  app_opens    BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (id)
);

Users individual device's brand name and registered user count and percentage of share of current device type compared to all device for the selected quarter of particular state
use phonepe_insights;

 CREATE TABLE aggregated_user_device_stats (
   id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
   state           VARCHAR(64)      NOT NULL,
   year            INT              NOT NULL,
   quarter         TINYINT          NOT NULL,
   user_brand      VARCHAR(64)      NOT NULL,
   user_count      BIGINT UNSIGNED  NOT NULL,
   user_percentage DECIMAL(10,6)    NOT NULL,
   PRIMARY KEY (id)
   
 )

<!-- --------------------------------------------------------------------------------------------------- -->
<!-- Dataset structure of aggregated Insurance data -->
{
    "success": true, //Ignore. For internal use only
    "code": "SUCCESS", //Ignore. For internal use only
    "data": {
        "from": 1609439400000, //Data duration
        "to": 1616869800000,
        "transactionData": [
            {
                "name": "Insurance", //Type of payment category
                "paymentInstruments": [
                    {
                        "type": "TOTAL",
                        "count": 318119, //Total number of insurance done for the above duration
                        "amount": 1.206307024 //Total value
                    }
                ]
            },
        ]
    },
    "responseTimestamp": 1630346628866 //Ignore. For internal use only.
}

 
d<!-- SQL table structur Insurance dataset -->
use phonepe_insights;

CREATE TABLE IF NOT EXISTS aggregated_insurance_stats (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
   state              VARCHAR(64)      NOT NULL,
   year               INT              NOT NULL,
  quarter            TINYINT          NOT NULL,
   insurance_type   VARCHAR(64)      NOT NULL,
   insurance_count  BIGINT UNSIGNED  NOT NULL,
   insurance_amount DECIMAL(20,2)    NOT NULL,
   PRIMARY KEY (id)
 );

## <!-- Mapped = Total values at the State and District levels. -->\
<!-- Dataset structure of mapped transaction data -->
Total number of transactions and total value of all transactions at the state level.
{
    "success": true, //Ignore. For internal use only.
    "code": "SUCCESS", //Ignore. For internal use only.
    "data": {
        "hoverDataList": [ //Internally, this being used to show state/district level data whenever a user hovers on a particular state/district.
            {
                "name": "puducherry", //State / district name
                "metric": [
                    {
                        "type": "TOTAL",
                        "count": 3309432, //Total number of transactions done within the selected year-quarter for the current state/district.
                        "amount": 5.899309571743641E9 //Total transaction value within the selected year-quarter for the current state/district.
                    }
                ]
            },

            ...,

            ...,

            {
                "name": "tamil nadu",
                "metric": [
                    {
                        "type": "TOTAL",
                        "count": 136556674,
                        "amount": 2.4866814387365314E11
                    }
                ]
            }
        ]
    },
    "responseTimestamp": 1630346628834 //Ignore. For internal use only.
}


 
<!-- SQL table structure of transaction data -->
USE phonepe_insights;

 CREATE TABLE mapped_transaction_district_totals (
   id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
   state              VARCHAR(64)     NOT NULL,  -- pulse key, e.g. 'tamil-nadu'
   district           VARCHAR(128)    NOT NULL,  -- as it appears in JSON: e.g. 'chennai'
   year               INT             NOT NULL,
   quarter            TINYINT         NOT NULL,  -- 1..4
   transaction_count  BIGINT UNSIGNED NOT NULL,
   transaction_amount DECIMAL(20,6)   NOT NULL,
   PRIMARY KEY (id)
 );

<!-- Dataset structure of mapped user data -->

Total number of registered users and number of app opens by these registered user
{
    "success": true, //Ignore. For internal use only.
    "code": "SUCCESS", //Ignore. For internal use only.
    "data": {
        "hoverData": { //Internally, this being used to show state/district level data whenever a user hovers on a particular state/district.
            "puducherry": {
                "registeredUsers": 346279, //Total number of registered users for the selected state/district
                "appOpens": 7914507 //Total number of app opens by the registered users for the selected state/district
            },

            ...,

            ...,

            "tamil nadu": {
                "registeredUsers": 16632608,
                "appOpens": 348801714
            }
        }
    },
    "responseTimestamp": 1630346628866 //Ignore. For internal use only.
}

<!-- SQL table structure of user data -->
use phonepe_insights;

CREATE TABLE IF NOT EXISTS mapped_user_devise_district (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    state VARCHAR(64)     NOT NULL, 
    district VARCHAR(128)    NOT NULL,
    year               INT             NOT NULL,
    registered_users BIGINT NOT NULL,
    app_opens BIGINT NOT NULL,
    quarter            TINYINT         NOT NULL,  -- 1..4
    PRIMARY KEY (id)
);





### In data_gathering.ipynb file, we have all the code which needed to modify the data and store in the database.