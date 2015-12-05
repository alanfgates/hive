package org.apache.hive.test.capybara.generated;
 
import org.apache.hive.test.capybara.IntegrationTest;
import org.apache.hive.test.capybara.infra.StatsDataGenerator;
import org.apache.hive.test.capybara.iface.TestTable;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
 
public class TestPandora extends IntegrationTest {
  private Map<String, TestTable> targetTables = new HashMap<>();
  @Before
  public void createTabledefault_dgies_tmp_pst_dev_l() throws Exception {
    TestTable tTable = TestTable.getBuilder("dgies_tmp_pst_dev_l")
        .setDbName("default")
        .addCol("dfp_advertiser_id", "bigint")
        .addCol("dfp_order_id", "bigint")
        .addCol("dfp_line_item_id", "bigint")
        .addCol("dfp_creative_id", "bigint")
        .addCol("listener_id", "bigint")
        .addCol("device_category", "string")
        .addCol("impressions", "bigint")
        .addCol("clicks", "bigint")
        .addCol("network_impressions", "bigint")
        .addCol("network_clicks", "bigint")
        .addCol("day", "string")
        .addCol("country_code", "string")
        .addCol("zip", "string")
        .addCol("birth_year", "int")
        .addCol("gender", "string")
        .build();
    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    colStats.put("dfp_advertiser_id",
      new StatsDataGenerator.ColStats("dfp_advertiser_id", "bigint", 
        -5646088590998929786L, 8213406531159434346L, 0, 0, 0, 0, 9, 0));
    colStats.put("dfp_order_id",
      new StatsDataGenerator.ColStats("dfp_order_id", "bigint", 
        -8130650289402224112L, 8301729436489095947L, 0, 0, 0, 0, 8, 0));
    colStats.put("dfp_line_item_id",
      new StatsDataGenerator.ColStats("dfp_line_item_id", "bigint", 
        -8477629219559858139L, 7350279913280329582L, 0, 0, 0, 0, 7, 1));
    colStats.put("dfp_creative_id",
      new StatsDataGenerator.ColStats("dfp_creative_id", "bigint", 
        -8855317047423422352L, 5324010061338526682L, 0, 0, 0, 0, 6, 0));
    colStats.put("listener_id",
      new StatsDataGenerator.ColStats("listener_id", "bigint", 
        -6457745460378519903L, 7792434490203218318L, 0, 0, 0, 0, 10, 0));
    colStats.put("device_category",
      new StatsDataGenerator.ColStats("device_category", "string", 
        null, null, 12.0, 18, 0, 0, 7, 0));
    colStats.put("impressions",
      new StatsDataGenerator.ColStats("impressions", "bigint", 
        -7733617943427577564L, 8779358894513985169L, 0, 0, 0, 0, 10, 0));
    colStats.put("clicks",
      new StatsDataGenerator.ColStats("clicks", "bigint", 
        -6340578576841064502L, 5734802180288610638L, 0, 0, 0, 0, 10, 0));
    colStats.put("network_impressions",
      new StatsDataGenerator.ColStats("network_impressions", "bigint", 
        -3661085812706355667L, 7515231975189841296L, 0, 0, 0, 0, 6, 0));
    colStats.put("network_clicks",
      new StatsDataGenerator.ColStats("network_clicks", "bigint", 
        -8977381472382413308L, 6755295306825439108L, 0, 0, 0, 0, 7, 0));
    colStats.put("day",
      new StatsDataGenerator.ColStats("day", "string", 
        null, null, 10.375, 20, 0, 0, 8, 0));
    colStats.put("country_code",
      new StatsDataGenerator.ColStats("country_code", "string", 
        null, null, 10.125, 20, 0, 0, 11, 0));
    colStats.put("zip",
      new StatsDataGenerator.ColStats("zip", "string", 
        null, null, 9.375, 15, 0, 0, 9, 0));
    colStats.put("birth_year",
      new StatsDataGenerator.ColStats("birth_year", "int", 
        -1449373051, 1925438281, 0, 0, 0, 0, 7, 0));
    colStats.put("gender",
      new StatsDataGenerator.ColStats("gender", "string", 
        null, null, 11.625, 20, 0, 0, 9, 0));
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 0, "default", "dgies_tmp_pst_dev_l", 2936, 8);
    StatsDataGenerator gen = new StatsDataGenerator(tableStats, 1068854363);
    tTable.create();
    tTable.populate(gen);
  }
  
  @Before
  public void createTabledefault_analytics_lds_utils_zip_state_legislative_districts() throws Exception {
    TestTable tTable = TestTable.getBuilder("analytics_lds_utils_zip_state_legislative_districts")
        .setDbName("default")
        .addCol("id", "int")
        .addCol("state", "string")
        .addCol("zip", "string")
        .addCol("district", "string")
        .addCol("upper_lower", "string")
        .build();
    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    colStats.put("id",
      new StatsDataGenerator.ColStats("id", "int", 
        -1868486257, 1761599140, 0, 0, 0, 0, 23, 0));
    colStats.put("state",
      new StatsDataGenerator.ColStats("state", "string", 
        null, null, 9.6957, 19, 0, 0, 20, 1));
    colStats.put("zip",
      new StatsDataGenerator.ColStats("zip", "string", 
        null, null, 11.4348, 19, 0, 0, 18, 0));
    colStats.put("district",
      new StatsDataGenerator.ColStats("district", "string", 
        null, null, 9.7391, 19, 0, 0, 21, 0));
    colStats.put("upper_lower",
      new StatsDataGenerator.ColStats("upper_lower", "string", 
        null, null, 10.7826, 20, 0, 0, 23, 0));
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 0, "default", "analytics_lds_utils_zip_state_legislative_districts", 5370, 23);
    StatsDataGenerator gen = new StatsDataGenerator(tableStats, -1204458921);
    tTable.create();
    tTable.populate(gen);
  }
  
  @Before
  public void createTabledefault_dgies_dfp2sst() throws Exception {
    TestTable tTable = TestTable.getBuilder("dgies_dfp2sst")
        .setDbName("default")
        .addCol("order_id", "bigint")
        .addCol("oline_id", "bigint")
        .addCol("flight_id", "bigint")
        .addCol("dfp_line_item_id", "bigint")
        .addCol("order_start_date", "string")
        .addCol("order_end_date", "string")
        .addCol("oline_start_date", "string")
        .addCol("oline_end_date", "string")
        .addCol("target_id", "bigint")
        .build();
    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    colStats.put("order_id",
      new StatsDataGenerator.ColStats("order_id", "bigint", 
        -184908662048528425L, 8406407647642205081L, 0, 0, 0, 0, 12, 0));
    colStats.put("oline_id",
      new StatsDataGenerator.ColStats("oline_id", "bigint", 
        -9061132763353265549L, 7477150374100206267L, 0, 0, 0, 0, 15, 0));
    colStats.put("flight_id",
      new StatsDataGenerator.ColStats("flight_id", "bigint", 
        -7934089718427420635L, 5170589409786284580L, 0, 0, 0, 0, 14, 1));
    colStats.put("dfp_line_item_id",
      new StatsDataGenerator.ColStats("dfp_line_item_id", "bigint", 
        -6700185377336963251L, 6929840818536435864L, 0, 0, 0, 0, 14, 0));
    colStats.put("order_start_date",
      new StatsDataGenerator.ColStats("order_start_date", "string", 
        null, null, 7.7143, 19, 0, 0, 11, 1));
    colStats.put("order_end_date",
      new StatsDataGenerator.ColStats("order_end_date", "string", 
        null, null, 9.0, 17, 0, 0, 15, 0));
    colStats.put("oline_start_date",
      new StatsDataGenerator.ColStats("oline_start_date", "string", 
        null, null, 10.3571, 20, 0, 0, 20, 0));
    colStats.put("oline_end_date",
      new StatsDataGenerator.ColStats("oline_end_date", "string", 
        null, null, 9.4286, 20, 0, 0, 16, 0));
    colStats.put("target_id",
      new StatsDataGenerator.ColStats("target_id", "bigint", 
        -8286327585465411592L, 7437081264419404432L, 0, 0, 0, 0, 14, 0));
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 0, "default", "dgies_dfp2sst", 3688, 14);
    StatsDataGenerator gen = new StatsDataGenerator(tableStats, 1325530938);
    tTable.create();
    tTable.populate(gen);
  }
  
  @Before
  public void createTabledefault_dgies_delivery_tracking_delta() throws Exception {
    TestTable tTable = TestTable.getBuilder("dgies_delivery_tracking_delta")
        .setDbName("default")
        .addCol("order_id", "bigint")
        .addCol("oline_id", "bigint")
        .addCol("late_effective_end_date", "boolean")
        .addCol("late_order_latest_delivery", "boolean")
        .addCol("has_effective_end_date", "boolean")
        .addCol("late_oline_end_date", "boolean")
        .addCol("late_oline_latest_delivery", "boolean")
        .addCol("has_oline_end_date", "boolean")
        .build();
    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    colStats.put("order_id",
      new StatsDataGenerator.ColStats("order_id", "bigint", 
        -9019826090389983362L, 8525887844183566721L, 0, 0, 0, 0, 56, 0));
    colStats.put("oline_id",
      new StatsDataGenerator.ColStats("oline_id", "bigint", 
        -9026332743112286068L, 8460583749768413651L, 0, 0, 0, 0, 53, 1));
    colStats.put("late_effective_end_date",
      new StatsDataGenerator.ColStats("late_effective_end_date", "boolean", 
        0, 0, 0, 0, 19, 28, 0, 0));
    colStats.put("late_order_latest_delivery",
      new StatsDataGenerator.ColStats("late_order_latest_delivery", "boolean", 
        0, 0, 0, 0, 20, 26, 0, 1));
    colStats.put("has_effective_end_date",
      new StatsDataGenerator.ColStats("has_effective_end_date", "boolean", 
        0, 0, 0, 0, 25, 22, 0, 0));
    colStats.put("late_oline_end_date",
      new StatsDataGenerator.ColStats("late_oline_end_date", "boolean", 
        0, 0, 0, 0, 25, 22, 0, 0));
    colStats.put("late_oline_latest_delivery",
      new StatsDataGenerator.ColStats("late_oline_latest_delivery", "boolean", 
        0, 0, 0, 0, 18, 29, 0, 0));
    colStats.put("has_oline_end_date",
      new StatsDataGenerator.ColStats("has_oline_end_date", "boolean", 
        0, 0, 0, 0, 20, 27, 0, 0));
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 0, "default", "dgies_delivery_tracking_delta", 1868, 47);
    StatsDataGenerator gen = new StatsDataGenerator(tableStats, -1971448722);
    tTable.create();
    tTable.populate(gen);
  }
  
  @Before
  public void createTabledefault_analytics_lds_utils_zip_county() throws Exception {
    TestTable tTable = TestTable.getBuilder("analytics_lds_utils_zip_county")
        .setDbName("default")
        .addCol("zip", "string")
        .addCol("county_id", "string")
        .addCol("county", "string")
        .build();
    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    colStats.put("zip",
      new StatsDataGenerator.ColStats("zip", "string", 
        null, null, 10.2571, 19, 0, 0, 36, 2));
    colStats.put("county_id",
      new StatsDataGenerator.ColStats("county_id", "string", 
        null, null, 8.9429, 20, 0, 0, 30, 0));
    colStats.put("county",
      new StatsDataGenerator.ColStats("county", "string", 
        null, null, 10.5714, 20, 0, 0, 37, 0));
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 0, "default", "analytics_lds_utils_zip_county", 5904, 35);
    StatsDataGenerator gen = new StatsDataGenerator(tableStats, 976200031);
    tTable.create();
    tTable.populate(gen);
  }
  
  @Before
  public void createTabledefault_analytics_tableau_zip_pop_temp() throws Exception {
    TestTable tTable = TestTable.getBuilder("analytics_tableau_zip_pop_temp")
        .setDbName("default")
        .addCol("zipcode", "string")
        .addCol("age", "int")
        .addCol("gender", "string")
        .addCol("population", "bigint")
        .build();
    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    colStats.put("zipcode",
      new StatsDataGenerator.ColStats("zipcode", "string", 
        null, null, 10.6667, 20, 0, 0, 36, 0));
    colStats.put("age",
      new StatsDataGenerator.ColStats("age", "int", 
        -1726202417, 2090425198, 0, 0, 0, 0, 31, 0));
    colStats.put("gender",
      new StatsDataGenerator.ColStats("gender", "string", 
        null, null, 11.5, 20, 0, 0, 18, 0));
    colStats.put("population",
      new StatsDataGenerator.ColStats("population", "bigint", 
        -8129328601469228981L, 8905052132846563547L, 0, 0, 0, 0, 22, 1));
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 0, "default", "analytics_tableau_zip_pop_temp", 3862, 30);
    StatsDataGenerator gen = new StatsDataGenerator(tableStats, 1691904534);
    tTable.create();
    tTable.populate(gen);
  }
  
  @Before
  public void createTabledefault_analytics_lds_utils_zip_state() throws Exception {
    TestTable tTable = TestTable.getBuilder("analytics_lds_utils_zip_state")
        .setDbName("default")
        .addCol("zip", "string")
        .addCol("state", "string")
        .build();
    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    colStats.put("zip",
      new StatsDataGenerator.ColStats("zip", "string", 
        null, null, 10.898, 20, 0, 0, 31, 1));
    colStats.put("state",
      new StatsDataGenerator.ColStats("state", "string", 
        null, null, 10.4694, 20, 0, 0, 58, 1));
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 0, "default", "analytics_lds_utils_zip_state", 5616, 49);
    StatsDataGenerator gen = new StatsDataGenerator(tableStats, -535535215);
    tTable.create();
    tTable.populate(gen);
  }
  
  @Before
  public void createTabledefault_dgies_delivered_oline_listener_device_yob_geos() throws Exception {
    TestTable tTable = TestTable.getBuilder("dgies_delivered_oline_listener_device_yob_geos")
        .setDbName("default")
        .addCol("dfp_advertiser_id", "bigint")
        .addCol("dfp_order_id", "bigint")
        .addCol("dfp_line_item_id", "bigint")
        .addCol("dfp_creative_id", "bigint")
        .addCol("order_id", "bigint")
        .addCol("oline_id", "bigint")
        .addCol("flight_id", "bigint")
        .addCol("listener_id", "bigint")
        .addCol("country_code", "string")
        .addCol("zip", "string")
        .addCol("birth_year", "int")
        .addCol("gender", "string")
        .addCol("device_category", "string")
        .addCol("is_demo_on_target", "boolean")
        .addCol("is_geo_on_target", "boolean")
        .addCol("is_date_in_range", "boolean")
        .addCol("impressions", "bigint")
        .addCol("clicks", "bigint")
        .addCol("network_impressions", "bigint")
        .addCol("network_clicks", "bigint")
        .addPartCol("active", "string")
        .addPartCol("day", "string")
        .setNumParts(1)
        .build();
    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    colStats.put("dfp_line_item_id",
      new StatsDataGenerator.ColStats("dfp_line_item_id", "bigint", 
        -7931671478478192739L, 8973385726267367868L, 0, 0, 0, 0, 14, 0));
    colStats.put("impressions",
      new StatsDataGenerator.ColStats("impressions", "bigint", 
        -6404324732952417905L, 9057697555194112261L, 0, 0, 0, 0, 13, 0));
    colStats.put("device_category",
      new StatsDataGenerator.ColStats("device_category", "string", 
        null, null, 11.75, 20, 0, 0, 11, 0));
    colStats.put("dfp_advertiser_id",
      new StatsDataGenerator.ColStats("dfp_advertiser_id", "bigint", 
        -7779217251056326553L, 8958702184462124147L, 0, 0, 0, 0, 9, 1));
    colStats.put("zip",
      new StatsDataGenerator.ColStats("zip", "string", 
        null, null, 9.1667, 15, 0, 0, 11, 0));
    colStats.put("network_impressions",
      new StatsDataGenerator.ColStats("network_impressions", "bigint", 
        -8031753008192788180L, 7847445616672799067L, 0, 0, 0, 0, 13, 0));
    colStats.put("is_geo_on_target",
      new StatsDataGenerator.ColStats("is_geo_on_target", "boolean", 
        0, 0, 0, 0, 6, 6, 0, 0));
    colStats.put("dfp_creative_id",
      new StatsDataGenerator.ColStats("dfp_creative_id", "bigint", 
        -9040789196411788323L, 8230372818103820087L, 0, 0, 0, 0, 12, 0));
    colStats.put("order_id",
      new StatsDataGenerator.ColStats("order_id", "bigint", 
        -8128954462062547057L, 6893990505568579099L, 0, 0, 0, 0, 12, 1));
    colStats.put("country_code",
      new StatsDataGenerator.ColStats("country_code", "string", 
        null, null, 10.8333, 20, 0, 0, 14, 1));
    colStats.put("birth_year",
      new StatsDataGenerator.ColStats("birth_year", "int", 
        -2111619271, 1997781750, 0, 0, 0, 0, 8, 1));
    colStats.put("network_clicks",
      new StatsDataGenerator.ColStats("network_clicks", "bigint", 
        -3001236973725174443L, 7948767809075583558L, 0, 0, 0, 0, 9, 1));
    colStats.put("is_date_in_range",
      new StatsDataGenerator.ColStats("is_date_in_range", "boolean", 
        0, 0, 0, 0, 6, 6, 0, 0));
    colStats.put("listener_id",
      new StatsDataGenerator.ColStats("listener_id", "bigint", 
        -7878127745186708523L, 8287317458733420412L, 0, 0, 0, 0, 11, 0));
    colStats.put("clicks",
      new StatsDataGenerator.ColStats("clicks", "bigint", 
        -7560738896330652349L, 9055384705204438462L, 0, 0, 0, 0, 14, 0));
    colStats.put("dfp_order_id",
      new StatsDataGenerator.ColStats("dfp_order_id", "bigint", 
        -4577183061323074090L, 9081007061746732813L, 0, 0, 0, 0, 11, 0));
    colStats.put("gender",
      new StatsDataGenerator.ColStats("gender", "string", 
        null, null, 12.0833, 20, 0, 0, 14, 0));
    colStats.put("oline_id",
      new StatsDataGenerator.ColStats("oline_id", "bigint", 
        -8078780802287979317L, 7863046514727263770L, 0, 0, 0, 0, 13, 0));
    colStats.put("flight_id",
      new StatsDataGenerator.ColStats("flight_id", "bigint", 
        -8844576721058183974L, 8556845475652632427L, 0, 0, 0, 0, 12, 0));
    colStats.put("is_demo_on_target",
      new StatsDataGenerator.ColStats("is_demo_on_target", "boolean", 
        0, 0, 0, 0, 3, 9, 0, 0));
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 1, "default", "dgies_delivered_oline_listener_device_yob_geos", 4077, 12);
    StatsDataGenerator gen = new StatsDataGenerator(tableStats, 302030928);
    tTable.create();
    tTable.populate(gen);
  }
  
  @Before
  public void createTabledefault_analytics_au_nz_geo_pop_120513() throws Exception {
    TestTable tTable = TestTable.getBuilder("analytics_au_nz_geo_pop_120513")
        .setDbName("default")
        .addCol("postal_code", "string")
        .addCol("region", "string")
        .addCol("region_code", "string")
        .addCol("country_code", "string")
        .addCol("market_code", "string")
        .addCol("market_name", "string")
        .build();
    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    colStats.put("postal_code",
      new StatsDataGenerator.ColStats("postal_code", "string", 
        null, null, 12.1765, 19, 0, 0, 20, 0));
    colStats.put("region",
      new StatsDataGenerator.ColStats("region", "string", 
        null, null, 8.1765, 20, 0, 0, 15, 0));
    colStats.put("region_code",
      new StatsDataGenerator.ColStats("region_code", "string", 
        null, null, 9.0588, 20, 0, 0, 16, 0));
    colStats.put("country_code",
      new StatsDataGenerator.ColStats("country_code", "string", 
        null, null, 10.6471, 19, 0, 0, 19, 0));
    colStats.put("market_code",
      new StatsDataGenerator.ColStats("market_code", "string", 
        null, null, 10.2353, 20, 0, 0, 24, 0));
    colStats.put("market_name",
      new StatsDataGenerator.ColStats("market_name", "string", 
        null, null, 10.7647, 20, 0, 0, 17, 0));
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 0, "default", "analytics_au_nz_geo_pop_120513", 5899, 17);
    StatsDataGenerator gen = new StatsDataGenerator(tableStats, 2130901228);
    tTable.create();
    tTable.populate(gen);
  }
  
  @Before
  public void createTabledefault_dgies_population_targets() throws Exception {
    TestTable tTable = TestTable.getBuilder("dgies_population_targets")
        .setDbName("default")
        .addCol("target_id", "bigint")
        .addCol("country_code", "string")
        .addCol("zip", "string")
        .addCol("age", "bigint")
        .addCol("gender", "string")
        .addCol("population", "bigint")
        .build();
    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    colStats.put("target_id",
      new StatsDataGenerator.ColStats("target_id", "bigint", 
        -8865988115086555182L, 6814797718813172012L, 0, 0, 0, 0, 16, 0));
    colStats.put("country_code",
      new StatsDataGenerator.ColStats("country_code", "string", 
        null, null, 12.7222, 19, 0, 0, 24, 0));
    colStats.put("zip",
      new StatsDataGenerator.ColStats("zip", "string", 
        null, null, 13.1111, 19, 0, 0, 13, 0));
    colStats.put("age",
      new StatsDataGenerator.ColStats("age", "bigint", 
        -7784304481409007085L, 9200271689706930264L, 0, 0, 0, 0, 20, 0));
    colStats.put("gender",
      new StatsDataGenerator.ColStats("gender", "string", 
        null, null, 9.2222, 18, 0, 0, 18, 0));
    colStats.put("population",
      new StatsDataGenerator.ColStats("population", "bigint", 
        -8792730803665656466L, 9161194822155447822L, 0, 0, 0, 0, 15, 1));
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 0, "default", "dgies_population_targets", 3628, 18);
    StatsDataGenerator gen = new StatsDataGenerator(tableStats, -373875412);
    tTable.createTargetTable();
    targetTables.put("dgies_population_targets", tTable);
  }
  
  @Before
  public void createTabledefault_dgies_cp_targ_map() throws Exception {
    TestTable tTable = TestTable.getBuilder("dgies_cp_targ_map")
        .setDbName("default")
        .addCol("oline_id", "bigint")
        .addCol("flight_id", "bigint")
        .addCol("updated_at", "string")
        .addCol("demo_target", "string")
        .addCol("geo_target", "string")
        .build();
    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    colStats.put("oline_id",
      new StatsDataGenerator.ColStats("oline_id", "bigint", 
        -8168310048487858771L, 7906527466031749325L, 0, 0, 0, 0, 25, 1));
    colStats.put("flight_id",
      new StatsDataGenerator.ColStats("flight_id", "bigint", 
        -9016975058078055352L, 7043572574383991330L, 0, 0, 0, 0, 24, 0));
    colStats.put("updated_at",
      new StatsDataGenerator.ColStats("updated_at", "string", 
        null, null, 8.0, 18, 0, 0, 22, 0));
    colStats.put("demo_target",
      new StatsDataGenerator.ColStats("demo_target", "string", 
        null, null, 9.875, 20, 0, 0, 29, 0));
    colStats.put("geo_target",
      new StatsDataGenerator.ColStats("geo_target", "string", 
        null, null, 9.9583, 20, 0, 0, 30, 1));
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 0, "default", "dgies_cp_targ_map", 4422, 24);
    StatsDataGenerator gen = new StatsDataGenerator(tableStats, -1350951291);
    tTable.create();
    tTable.populate(gen);
  }
  
  @Before
  public void createTabledefault_dgies_cp_new_targets() throws Exception {
    TestTable tTable = TestTable.getBuilder("dgies_cp_new_targets")
        .setDbName("default")
        .addCol("id", "bigint")
        .addCol("demo_target", "string")
        .addCol("geo_target", "string")
        .addCol("updated_at", "string")
        .build();
    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    colStats.put("id",
      new StatsDataGenerator.ColStats("id", "bigint", 
        -7183425886221605438L, 9155915128389661625L, 0, 0, 0, 0, 22, 0));
    colStats.put("demo_target",
      new StatsDataGenerator.ColStats("demo_target", "string", 
        null, null, 10.3448, 20, 0, 0, 24, 0));
    colStats.put("geo_target",
      new StatsDataGenerator.ColStats("geo_target", "string", 
        null, null, 8.6897, 20, 0, 0, 28, 0));
    colStats.put("updated_at",
      new StatsDataGenerator.ColStats("updated_at", "string", 
        null, null, 8.6552, 20, 0, 0, 29, 0));
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 0, "default", "dgies_cp_new_targets", 5162, 29);
    StatsDataGenerator gen = new StatsDataGenerator(tableStats, -178471998);
    tTable.create();
    tTable.populate(gen);
  }
  
  @Before
  public void createTabledefault_dgies_olines_country_targets() throws Exception {
    TestTable tTable = TestTable.getBuilder("dgies_olines_country_targets")
        .setDbName("default")
        .addCol("oline_id", "bigint")
        .addCol("country_target", "string")
        .build();
    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    colStats.put("oline_id",
      new StatsDataGenerator.ColStats("oline_id", "bigint", 
        -9026595475074594255L, 8715403918205548067L, 0, 0, 0, 0, 56, 1));
    colStats.put("country_target",
      new StatsDataGenerator.ColStats("country_target", "string", 
        null, null, 10.8364, 20, 0, 0, 66, 2));
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 0, "default", "dgies_olines_country_targets", 3559, 55);
    StatsDataGenerator gen = new StatsDataGenerator(tableStats, 1117215582);
    tTable.create();
    tTable.populate(gen);
  }
  
  @Before
  public void createTabledefault_analytics_lds_utils_zip_federal_congressional_districts() throws Exception {
    TestTable tTable = TestTable.getBuilder("analytics_lds_utils_zip_federal_congressional_districts")
        .setDbName("default")
        .addCol("id", "int")
        .addCol("state", "string")
        .addCol("zip", "string")
        .addCol("district", "string")
        .build();
    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    colStats.put("id",
      new StatsDataGenerator.ColStats("id", "int", 
        -2027142318, 2123776725, 0, 0, 0, 0, 26, 0));
    colStats.put("state",
      new StatsDataGenerator.ColStats("state", "string", 
        null, null, 10.4074, 20, 0, 0, 37, 0));
    colStats.put("zip",
      new StatsDataGenerator.ColStats("zip", "string", 
        null, null, 12.0741, 20, 0, 0, 24, 0));
    colStats.put("district",
      new StatsDataGenerator.ColStats("district", "string", 
        null, null, 11.963, 20, 0, 0, 31, 0));
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 0, "default", "analytics_lds_utils_zip_federal_congressional_districts", 4887, 27);
    StatsDataGenerator gen = new StatsDataGenerator(tableStats, 776585252);
    tTable.create();
    tTable.populate(gen);
  }
  
  @Before
  public void createTabledefault_analytics_lds_utils_zip_msa() throws Exception {
    TestTable tTable = TestTable.getBuilder("analytics_lds_utils_zip_msa")
        .setDbName("default")
        .addCol("zip", "string")
        .addCol("msa_id", "string")
        .addCol("msa_name", "string")
        .build();
    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    colStats.put("zip",
      new StatsDataGenerator.ColStats("zip", "string", 
        null, null, 9.5312, 20, 0, 0, 39, 0));
    colStats.put("msa_id",
      new StatsDataGenerator.ColStats("msa_id", "string", 
        null, null, 12.9375, 20, 0, 0, 30, 0));
    colStats.put("msa_name",
      new StatsDataGenerator.ColStats("msa_name", "string", 
        null, null, 10.3125, 20, 0, 0, 39, 0));
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 0, "default", "analytics_lds_utils_zip_msa", 5600, 32);
    StatsDataGenerator gen = new StatsDataGenerator(tableStats, -1365533085);
    tTable.create();
    tTable.populate(gen);
  }
  
  @Before
  public void createTabledefault_dgies_tmp_pst() throws Exception {
    TestTable tTable = TestTable.getBuilder("dgies_tmp_pst")
        .setDbName("default")
        .addCol("dfp_advertiser_id", "bigint")
        .addCol("dfp_order_id", "bigint")
        .addCol("dfp_line_item_id", "bigint")
        .addCol("dfp_creative_id", "bigint")
        .addCol("listener_id", "bigint")
        .addCol("device_id", "string")
        .addCol("vendor_id", "int")
        .addCol("impressions", "bigint")
        .addCol("clicks", "bigint")
        .addCol("network_impressions", "bigint")
        .addCol("network_clicks", "bigint")
        .addCol("day", "string")
        .build();
    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    colStats.put("dfp_advertiser_id",
      new StatsDataGenerator.ColStats("dfp_advertiser_id", "bigint", 
        -7386335749782694253L, 7863285468683452516L, 0, 0, 0, 0, 8, 1));
    colStats.put("dfp_order_id",
      new StatsDataGenerator.ColStats("dfp_order_id", "bigint", 
        -6779824969268577632L, 8632506437885874506L, 0, 0, 0, 0, 14, 0));
    colStats.put("dfp_line_item_id",
      new StatsDataGenerator.ColStats("dfp_line_item_id", "bigint", 
        -9067857255685900952L, 8613472514764210842L, 0, 0, 0, 0, 7, 1));
    colStats.put("dfp_creative_id",
      new StatsDataGenerator.ColStats("dfp_creative_id", "bigint", 
        -6738450871494154373L, 3508314030055549734L, 0, 0, 0, 0, 12, 0));
    colStats.put("listener_id",
      new StatsDataGenerator.ColStats("listener_id", "bigint", 
        -8945841630318512185L, 8068318529774729172L, 0, 0, 0, 0, 12, 0));
    colStats.put("device_id",
      new StatsDataGenerator.ColStats("device_id", "string", 
        null, null, 9.6364, 20, 0, 0, 12, 0));
    colStats.put("vendor_id",
      new StatsDataGenerator.ColStats("vendor_id", "int", 
        -1921607397, 1470101413, 0, 0, 0, 0, 14, 0));
    colStats.put("impressions",
      new StatsDataGenerator.ColStats("impressions", "bigint", 
        -8051846362009575073L, 9066187912879441015L, 0, 0, 0, 0, 14, 0));
    colStats.put("clicks",
      new StatsDataGenerator.ColStats("clicks", "bigint", 
        -6078524638008869068L, 8450707878781417331L, 0, 0, 0, 0, 12, 0));
    colStats.put("network_impressions",
      new StatsDataGenerator.ColStats("network_impressions", "bigint", 
        -6363799459712596570L, 7540310766316254785L, 0, 0, 0, 0, 11, 0));
    colStats.put("network_clicks",
      new StatsDataGenerator.ColStats("network_clicks", "bigint", 
        -8533831604039603803L, 7828850377858373362L, 0, 0, 0, 0, 10, 0));
    colStats.put("day",
      new StatsDataGenerator.ColStats("day", "string", 
        null, null, 9.0909, 19, 0, 0, 14, 0));
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 0, "default", "dgies_tmp_pst", 2074, 11);
    StatsDataGenerator gen = new StatsDataGenerator(tableStats, 1539086535);
    tTable.create();
    tTable.populate(gen);
  }
  
  @Before
  public void createTabledefault_dgies_geo_rollup() throws Exception {
    TestTable tTable = TestTable.getBuilder("dgies_geo_rollup")
        .setDbName("default")
        .addCol("country_code", "string")
        .addCol("postal_code", "string")
        .addCol("dma_ids", "string")
        .addCol("msa_ids", "string")
        .addCol("county_ids", "string")
        .addCol("state_names", "string")
        .addCol("congressional_districts", "string")
        .addCol("state_districts", "string")
        .addCol("au_state_names", "string")
        .addCol("nz_region_names", "string")
        .addCol("au_market_ids", "string")
        .build();
    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    colStats.put("country_code",
      new StatsDataGenerator.ColStats("country_code", "string", 
        null, null, 10.7778, 20, 0, 0, 8, 0));
    colStats.put("postal_code",
      new StatsDataGenerator.ColStats("postal_code", "string", 
        null, null, 7.4444, 19, 0, 0, 10, 0));
    colStats.put("dma_ids",
      new StatsDataGenerator.ColStats("dma_ids", "string", 
        null, null, 11.0, 20, 0, 0, 9, 0));
    colStats.put("msa_ids",
      new StatsDataGenerator.ColStats("msa_ids", "string", 
        null, null, 9.2222, 17, 0, 0, 9, 0));
    colStats.put("county_ids",
      new StatsDataGenerator.ColStats("county_ids", "string", 
        null, null, 14.2222, 20, 0, 0, 12, 0));
    colStats.put("state_names",
      new StatsDataGenerator.ColStats("state_names", "string", 
        null, null, 9.5556, 20, 0, 0, 12, 0));
    colStats.put("congressional_districts",
      new StatsDataGenerator.ColStats("congressional_districts", "string", 
        null, null, 11.7778, 20, 0, 0, 10, 0));
    colStats.put("state_districts",
      new StatsDataGenerator.ColStats("state_districts", "string", 
        null, null, 12.0, 20, 0, 0, 9, 0));
    colStats.put("au_state_names",
      new StatsDataGenerator.ColStats("au_state_names", "string", 
        null, null, 11.3333, 19, 0, 0, 9, 0));
    colStats.put("nz_region_names",
      new StatsDataGenerator.ColStats("nz_region_names", "string", 
        null, null, 11.2222, 17, 0, 0, 9, 0));
    colStats.put("au_market_ids",
      new StatsDataGenerator.ColStats("au_market_ids", "string", 
        null, null, 10.1111, 16, 0, 0, 12, 0));
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 0, "default", "dgies_geo_rollup", 5787, 9);
    StatsDataGenerator gen = new StatsDataGenerator(tableStats, -1748779770);
    tTable.create();
    tTable.populate(gen);
  }
  
  @Before
  public void createTabledefault_analytics_d_accessory() throws Exception {
    TestTable tTable = TestTable.getBuilder("analytics_d_accessory")
        .setDbName("default")
        .addCol("accdevven", "string")
        .addCol("accessory_id", "int")
        .addCol("device_id", "string")
        .addCol("vendor_id", "int")
        .addCol("listener_accessory_code", "string")
        .addCol("listener_device_name", "string")
        .addCol("listener_vendor_name", "string")
        .addCol("acc_category", "string")
        .addCol("acc_type", "string")
        .addCol("dev_category", "string")
        .addCol("dev_type", "string")
        .addCol("dev_reporting_vendor", "string")
        .addCol("ven_category", "string")
        .addCol("ven_type", "string")
        .addCol("ven_reporting_vendor", "string")
        .addCol("category", "string")
        .addCol("type", "string")
        .addCol("vendor_name", "string")
        .addCol("reporting_vendor", "string")
        .addCol("vendor_display_name", "string")
        .addCol("device_display_name", "string")
        .addCol("parent_brand", "string")
        .addCol("region_sold", "string")
        .build();
    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    colStats.put("accdevven",
      new StatsDataGenerator.ColStats("accdevven", "string", 
        null, null, 10.4, 19, 0, 0, 5, 0));
    colStats.put("accessory_id",
      new StatsDataGenerator.ColStats("accessory_id", "int", 
        -2106129025, 191375186, 0, 0, 0, 0, 4, 0));
    colStats.put("device_id",
      new StatsDataGenerator.ColStats("device_id", "string", 
        null, null, 7.4, 20, 0, 0, 5, 0));
    colStats.put("vendor_id",
      new StatsDataGenerator.ColStats("vendor_id", "int", 
        -1680538035, 1864975943, 0, 0, 0, 0, 4, 0));
    colStats.put("listener_accessory_code",
      new StatsDataGenerator.ColStats("listener_accessory_code", "string", 
        null, null, 5.8, 11, 0, 0, 5, 0));
    colStats.put("listener_device_name",
      new StatsDataGenerator.ColStats("listener_device_name", "string", 
        null, null, 12.0, 17, 0, 0, 5, 0));
    colStats.put("listener_vendor_name",
      new StatsDataGenerator.ColStats("listener_vendor_name", "string", 
        null, null, 5.8, 10, 0, 0, 7, 0));
    colStats.put("acc_category",
      new StatsDataGenerator.ColStats("acc_category", "string", 
        null, null, 11.8, 16, 0, 0, 5, 0));
    colStats.put("acc_type",
      new StatsDataGenerator.ColStats("acc_type", "string", 
        null, null, 9.8, 18, 0, 0, 5, 0));
    colStats.put("dev_category",
      new StatsDataGenerator.ColStats("dev_category", "string", 
        null, null, 6.8, 20, 0, 0, 3, 0));
    colStats.put("dev_type",
      new StatsDataGenerator.ColStats("dev_type", "string", 
        null, null, 10.6, 15, 0, 0, 5, 0));
    colStats.put("dev_reporting_vendor",
      new StatsDataGenerator.ColStats("dev_reporting_vendor", "string", 
        null, null, 10.4, 17, 0, 0, 6, 0));
    colStats.put("ven_category",
      new StatsDataGenerator.ColStats("ven_category", "string", 
        null, null, 7.0, 13, 0, 0, 5, 0));
    colStats.put("ven_type",
      new StatsDataGenerator.ColStats("ven_type", "string", 
        null, null, 8.2, 15, 0, 0, 4, 0));
    colStats.put("ven_reporting_vendor",
      new StatsDataGenerator.ColStats("ven_reporting_vendor", "string", 
        null, null, 10.2, 17, 0, 0, 4, 0));
    colStats.put("category",
      new StatsDataGenerator.ColStats("category", "string", 
        null, null, 10.6, 20, 0, 0, 6, 0));
    colStats.put("type",
      new StatsDataGenerator.ColStats("type", "string", 
        null, null, 7.2, 20, 0, 0, 5, 0));
    colStats.put("vendor_name",
      new StatsDataGenerator.ColStats("vendor_name", "string", 
        null, null, 9.2, 15, 0, 0, 6, 0));
    colStats.put("reporting_vendor",
      new StatsDataGenerator.ColStats("reporting_vendor", "string", 
        null, null, 13.6, 19, 0, 0, 6, 0));
    colStats.put("vendor_display_name",
      new StatsDataGenerator.ColStats("vendor_display_name", "string", 
        null, null, 13.6, 20, 0, 0, 5, 0));
    colStats.put("device_display_name",
      new StatsDataGenerator.ColStats("device_display_name", "string", 
        null, null, 7.8, 16, 0, 0, 6, 0));
    colStats.put("parent_brand",
      new StatsDataGenerator.ColStats("parent_brand", "string", 
        null, null, 12.4, 20, 0, 0, 7, 0));
    colStats.put("region_sold",
      new StatsDataGenerator.ColStats("region_sold", "string", 
        null, null, 11.2, 17, 0, 0, 4, 0));
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 0, "default", "analytics_d_accessory", 6040, 5);
    StatsDataGenerator gen = new StatsDataGenerator(tableStats, -1783718673);
    tTable.create();
    tTable.populate(gen);
  }
  
  @Before
  public void createTabledefault_jmoffitt_au_nz_geo_pop_120513() throws Exception {
    TestTable tTable = TestTable.getBuilder("jmoffitt_au_nz_geo_pop_120513")
        .setDbName("default")
        .addCol("postal_code", "string")
        .addCol("region", "string")
        .addCol("region_code", "string")
        .addCol("country_code", "string")
        .addCol("market_code", "string")
        .addCol("market_name", "string")
        .addCol("gender", "string")
        .addCol("age", "string")
        .addCol("population", "int")
        .build();
    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    colStats.put("postal_code",
      new StatsDataGenerator.ColStats("postal_code", "string", 
        null, null, 8.8462, 16, 0, 0, 15, 0));
    colStats.put("region",
      new StatsDataGenerator.ColStats("region", "string", 
        null, null, 6.9231, 17, 0, 0, 16, 0));
    colStats.put("region_code",
      new StatsDataGenerator.ColStats("region_code", "string", 
        null, null, 9.1538, 20, 0, 0, 18, 0));
    colStats.put("country_code",
      new StatsDataGenerator.ColStats("country_code", "string", 
        null, null, 10.0769, 19, 0, 0, 16, 0));
    colStats.put("market_code",
      new StatsDataGenerator.ColStats("market_code", "string", 
        null, null, 11.7692, 20, 0, 0, 14, 0));
    colStats.put("market_name",
      new StatsDataGenerator.ColStats("market_name", "string", 
        null, null, 11.1538, 19, 0, 0, 12, 0));
    colStats.put("gender",
      new StatsDataGenerator.ColStats("gender", "string", 
        null, null, 10.9231, 20, 0, 0, 12, 0));
    colStats.put("age",
      new StatsDataGenerator.ColStats("age", "string", 
        null, null, 9.3846, 17, 0, 0, 15, 0));
    colStats.put("population",
      new StatsDataGenerator.ColStats("population", "int", 
        -1979708418, 1802889094, 0, 0, 0, 0, 15, 0));
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 0, "default", "jmoffitt_au_nz_geo_pop_120513", 6006, 13);
    StatsDataGenerator gen = new StatsDataGenerator(tableStats, 1167386918);
    tTable.create();
    tTable.populate(gen);
  }
  
  @Before
  public void createTabledefault_dgies_geographies() throws Exception {
    TestTable tTable = TestTable.getBuilder("dgies_geographies")
        .setDbName("default")
        .addCol("country_code", "string")
        .addCol("postal_code", "string")
        .addCol("dma_id", "string")
        .addCol("dma_name", "string")
        .addCol("msa_id", "string")
        .addCol("msa_name", "string")
        .addCol("msa_duplicate", "boolean")
        .addCol("county_id", "string")
        .addCol("county_name", "string")
        .addCol("state_name", "string")
        .addCol("congressional_district", "string")
        .addCol("congressional_duplicate", "boolean")
        .addCol("state_upper", "string")
        .addCol("upper_duplicate", "boolean")
        .addCol("state_lower", "string")
        .addCol("lower_duplicate", "boolean")
        .addCol("au_state", "string")
        .addCol("nz_region", "string")
        .addCol("au_market_id", "string")
        .addCol("au_market_name", "string")
        .build();
    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    colStats.put("country_code",
      new StatsDataGenerator.ColStats("country_code", "string", 
        null, null, 7.8571, 15, 0, 0, 9, 0));
    colStats.put("postal_code",
      new StatsDataGenerator.ColStats("postal_code", "string", 
        null, null, 11.0, 19, 0, 0, 5, 0));
    colStats.put("dma_id",
      new StatsDataGenerator.ColStats("dma_id", "string", 
        null, null, 8.4286, 13, 0, 0, 6, 0));
    colStats.put("dma_name",
      new StatsDataGenerator.ColStats("dma_name", "string", 
        null, null, 10.4286, 19, 0, 0, 7, 0));
    colStats.put("msa_id",
      new StatsDataGenerator.ColStats("msa_id", "string", 
        null, null, 8.8571, 18, 0, 0, 7, 0));
    colStats.put("msa_name",
      new StatsDataGenerator.ColStats("msa_name", "string", 
        null, null, 8.1429, 18, 0, 0, 7, 0));
    colStats.put("msa_duplicate",
      new StatsDataGenerator.ColStats("msa_duplicate", "boolean", 
        0, 0, 0, 0, 4, 3, 0, 0));
    colStats.put("county_id",
      new StatsDataGenerator.ColStats("county_id", "string", 
        null, null, 10.0, 18, 0, 0, 7, 0));
    colStats.put("county_name",
      new StatsDataGenerator.ColStats("county_name", "string", 
        null, null, 12.0, 18, 0, 0, 9, 0));
    colStats.put("state_name",
      new StatsDataGenerator.ColStats("state_name", "string", 
        null, null, 10.5714, 16, 0, 0, 7, 0));
    colStats.put("congressional_district",
      new StatsDataGenerator.ColStats("congressional_district", "string", 
        null, null, 12.4286, 18, 0, 0, 8, 0));
    colStats.put("congressional_duplicate",
      new StatsDataGenerator.ColStats("congressional_duplicate", "boolean", 
        0, 0, 0, 0, 4, 3, 0, 0));
    colStats.put("state_upper",
      new StatsDataGenerator.ColStats("state_upper", "string", 
        null, null, 5.7143, 14, 0, 0, 9, 0));
    colStats.put("upper_duplicate",
      new StatsDataGenerator.ColStats("upper_duplicate", "boolean", 
        0, 0, 0, 0, 5, 2, 0, 0));
    colStats.put("state_lower",
      new StatsDataGenerator.ColStats("state_lower", "string", 
        null, null, 12.0, 18, 0, 0, 6, 0));
    colStats.put("lower_duplicate",
      new StatsDataGenerator.ColStats("lower_duplicate", "boolean", 
        0, 0, 0, 0, 0, 7, 0, 0));
    colStats.put("au_state",
      new StatsDataGenerator.ColStats("au_state", "string", 
        null, null, 12.4286, 18, 0, 0, 6, 0));
    colStats.put("nz_region",
      new StatsDataGenerator.ColStats("nz_region", "string", 
        null, null, 7.4286, 20, 0, 0, 7, 1));
    colStats.put("au_market_id",
      new StatsDataGenerator.ColStats("au_market_id", "string", 
        null, null, 12.5714, 20, 0, 0, 6, 0));
    colStats.put("au_market_name",
      new StatsDataGenerator.ColStats("au_market_name", "string", 
        null, null, 9.5714, 16, 0, 0, 7, 0));
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 0, "default", "dgies_geographies", 6510, 7);
    StatsDataGenerator gen = new StatsDataGenerator(tableStats, -1449023577);
    tTable.createTargetTable();
    targetTables.put("dgies_geographies", tTable);
  }
  
  @Before
  public void createTabledefault_analytics_lds_utils_zip_dma() throws Exception {
    TestTable tTable = TestTable.getBuilder("analytics_lds_utils_zip_dma")
        .setDbName("default")
        .addCol("zip", "string")
        .addCol("dma_id", "string")
        .addCol("dma", "string")
        .build();
    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    colStats.put("zip",
      new StatsDataGenerator.ColStats("zip", "string", 
        null, null, 9.8235, 20, 0, 0, 45, 0));
    colStats.put("dma_id",
      new StatsDataGenerator.ColStats("dma_id", "string", 
        null, null, 9.5882, 19, 0, 0, 37, 2));
    colStats.put("dma",
      new StatsDataGenerator.ColStats("dma", "string", 
        null, null, 10.8529, 20, 0, 0, 39, 0));
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 0, "default", "analytics_lds_utils_zip_dma", 5766, 34);
    StatsDataGenerator gen = new StatsDataGenerator(tableStats, 1127516602);
    tTable.create();
    tTable.populate(gen);
  }
  
  @Test
  public void q6() throws Exception {
    set("hive.auto.convert.join", "true");
    set("hive.exec.reducers.max", "6400");
    runQuery(
        "drop table if exists dgies_tmp_pst_dev_l_ss"
    );
    runQuery(
        "create table if not exists dgies_tmp_pst_dev_l_ss ("
        + "dfp_advertiser_id   bigint,"
        + "dfp_order_id        bigint,"
        + "dfp_line_item_id    bigint,"
        + "dfp_creative_id     bigint,"
        + "listener_id         bigint,"
        + "device_category     string,"
        + "impressions         bigint,"
        + "clicks              bigint,"
        + "network_impressions bigint,"
        + "network_clicks      bigint,"
        + "day                 string,"
        + "country_code        string,"
        + "zip                 string,"
        + "birth_year          int,"
        + "gender              string,"
        + "order_id            bigint,"
        + "oline_id            bigint,"
        + "flight_id           bigint,"
        + "target_id           bigint,"
        + "is_date_in_range    boolean"
        + ")"
        + "stored as orc"
    );
    runQuery(
        "insert overwrite table dgies_tmp_pst_dev_l_ss"
        + "select"
        + "pstdevl.dfp_advertiser_id,"
        + "pstdevl.dfp_order_id,"
        + "pstdevl.dfp_line_item_id,"
        + "pstdevl.dfp_creative_id,"
        + "pstdevl.listener_id,"
        + "pstdevl.device_category,"
        + "pstdevl.impressions,"
        + "pstdevl.clicks,"
        + "pstdevl.network_impressions,"
        + "pstdevl.network_clicks,"
        + "pstdevl.day,"
        + "pstdevl.country_code,"
        + "pstdevl.zip,"
        + "pstdevl.birth_year,"
        + "pstdevl.gender,"
        + "dfp2sst.order_id,"
        + "dfp2sst.oline_id,"
        + "dfp2sst.flight_id,"
        + "dfp2sst.target_id,"
        + "if("
        + "dfp2sst.oline_start_date is not null and dfp2sst.oline_end_date is not null,"
        + "pstdevl.day >= dfp2sst.oline_start_date and pstdevl.day <= dfp2sst.oline_end_date,"
        + "true"
        + ") as is_date_in_range"
        + "from dgies_tmp_pst_dev_l pstdevl"
        + "left outer join dgies_dfp2sst dfp2sst"
        + "on pstdevl.dfp_line_item_id = dfp2sst.dfp_line_item_id"
    );
    String dbName, tableOnlyName;
    dbName = "default";
    tableOnlyName = "dgies_tmp_pst_dev_l_ss";
    TestTable tTable = TestTable.fromHiveMetastore(dbName, tableOnlyName);
    tableCompare(tTable);
  }
  
  @Test
  public void q5() throws Exception {
    runQuery(
        "drop table if exists dgies_tmp_gp"
    );
    runQuery(
        "create table dgies_tmp_gp ("
        + "country_code            string,"
        + "postal_code             string,"
        + "dma_ids                 string,"
        + "msa_ids                 string,"
        + "county_ids              string,"
        + "state_names             string,"
        + "congressional_districts string,"
        + "state_districts         string,"
        + "au_state_names          string,"
        + "nz_region_names         string,"
        + "au_market_ids           string,"
        + "age                     int,"
        + "gender                  string,"
        + "population              bigint"
        + ")"
        + "stored as textfile"
    );
    set("hive.exec.compress.output", "false");
    set("hive.auto.convert.join", "true");
    runQuery(
        "insert overwrite table dgies_tmp_gp"
        + "select * from ("
        + "select"
        + "geo_rollup.*,"
        + "zippop.age,"
        + "zippop.gender,"
        + "zippop.population"
        + "from analytics_tableau_zip_pop_temp zippop"
        + "inner join dgies_geo_rollup geo_rollup"
        + "on  zippop.zipcode =  geo_rollup.postal_code"
        + "and geo_rollup.country_code = 'us'"
        + "where zippop.population > 0"
        + ""
        + "union all"
        + "select"
        + "geo_rollup.*,"
        + "au_geo_pop.age,"
        + "au_geo_pop.gender,"
        + "au_geo_pop.population"
        + "from jmoffitt_au_nz_geo_pop_120513 au_geo_pop"
        + "inner join dgies_geo_rollup geo_rollup"
        + "on  au_geo_pop.postal_code =  geo_rollup.postal_code"
        + "and geo_rollup.country_code = 'au'"
        + "where au_geo_pop.population > 0"
        + "and   au_geo_pop.age not like '%and%'"
        + ""
        + ")"
        + "cluster by rand()"
    );
    set("hive.exec.compress.output", "true");
    runQuery(
        "create table if not exists dgies_population_targets ("
        + "target_id           bigint,"
        + "country_code        string,"
        + "zip                 string,"
        + "age                 bigint,"
        + "gender              string,"
        + "population          bigint"
        + ")"
        + "stored as orc"
    );
    runQuery(
        ""
        + "set hive.hadoop.supports.splittable.combineinputformat=false"
    );
    set("mapreduce.input.fileinputformat.split.maxsize", "100000");
    set("mapred.max.split.size", "100000");
    runQuery(
        ""
        + "set hive.exec.reducers.bytes.per.reducer=500000"
    );
    set("hive.exec.reducers.max", "3200");
    runQuery(
        ""
        + "set mapreduce.job.reduce.slowstart.completedmaps=0.999"
    );
    runQuery(
        ""
        + "set hive.auto.convert.join=true"
    );
    set("hive.auto.convert.join.noconditionaltask", "true");
    runQuery(
        "insert into table dgies_population_targets"
        + "select /*+ mapjoin(dgies_cp_new_targets) */"
        + "targets.id         as target_id,"
        + "geos.country_code,"
        + "geos.postal_code   as zip,"
        + "geos.age,"
        + "geos.gender,"
        + "geos.population"
        + "from dgies_tmp_gp geos"
        + ""
        + "inner join dgies_cp_new_targets targets"
        + "on 1=1"
        + "where"
        + "isdemoontarget("
        + "geos.age,"
        + "coalesce(geos.gender, 'unknown'),"
        + "coalesce(targets.demo_target, '')"
        + ")"
        + "and"
        + "isgeoontarget("
        + "geos.country_code,"
        + "geos.postal_code,"
        + "geos.dma_ids,"
        + "geos.msa_ids,"
        + "geos.county_ids,"
        + "geos.state_names,"
        + "geos.congressional_districts,"
        + "geos.state_districts,"
        + "geos.au_state_names,"
        + "geos.nz_region_names,"
        + "geos.au_market_ids,"
        + "coalesce(targets.geo_target, '')"
        + ")"
        + ""
        + ""
        + "cluster by rand()"
    );
    TestTable tTable = targetTables.get("dgies_population_targets");
    tableCompare(tTable);
  }
  
  @Test
  public void q7() throws Exception {
    set("hive.auto.convert.join", "true");
    set("hive.exec.reducers.bytes.per.reducer", "100000000");
    set("hive.hadoop.supports.splittable.combineinputformat", "false");
    set("hive.exec.reducers.max", "6400");
    runQuery(
        "drop table if exists dgies_tmp_pst_dev"
    );
    runQuery(
        "create table if not exists dgies_tmp_pst_dev ("
        + "dfp_advertiser_id   bigint,"
        + "dfp_order_id        bigint,"
        + "dfp_line_item_id    bigint,"
        + "dfp_creative_id     bigint,"
        + "listener_id         bigint,"
        + "device_category     string,"
        + "impressions         bigint,"
        + "clicks              bigint,"
        + "network_impressions bigint,"
        + "network_clicks      bigint,"
        + "day string"
        + ")"
        + "stored as textfile"
    );
    runQuery(
        "insert overwrite table dgies_tmp_pst_dev"
        + "select /*+ mapjoin(analytics_d_accessory) */"
        + "pst.dfp_advertiser_id,"
        + "pst.dfp_order_id,"
        + "pst.dfp_line_item_id,"
        + "pst.dfp_creative_id,"
        + "pst.listener_id,"
        + "case"
        + "when dev.type = 'ios'     and dev.category = 'smartphones'     then 'iphone'"
        + "when dev.type = 'ios'     and dev.category = 'tablet_e_reader' then 'ipad'"
        + "when dev.type = 'android' and dev.category = 'smartphones'     then 'android mobile'"
        + "when dev.type = 'android' and dev.category = 'tablet_e_reader' then 'android tablet'"
        + "when dev.category is not null                                  then dev.category"
        + "else 'unknown'"
        + "end                           as device_category,"
        + "sum(pst.impressions) as impressions,"
        + "sum(pst.clicks) as clicks,"
        + "sum(pst.network_impressions) as network_impressions,"
        + "sum(pst.network_clicks) as network_clicks,"
        + "pst.day"
        + "from  dgies_tmp_pst      pst"
        + "left outer join analytics_d_accessory dev"
        + "on concat(dev.device_id, '-', dev.vendor_id, '-', dev.accessory_id)"
        + "= concat(coalesce(pst.device_id, -1), '-', coalesce(pst.vendor_id, -1), '-', '-1')"
        + "group by"
        + "pst.dfp_advertiser_id,"
        + "pst.dfp_order_id,"
        + "pst.dfp_line_item_id,"
        + "pst.dfp_creative_id,"
        + "pst.listener_id,"
        + "case"
        + "when dev.type = 'ios'     and dev.category = 'smartphones'     then 'iphone'"
        + "when dev.type = 'ios'     and dev.category = 'tablet_e_reader' then 'ipad'"
        + "when dev.type = 'android' and dev.category = 'smartphones'     then 'android mobile'"
        + "when dev.type = 'android' and dev.category = 'tablet_e_reader' then 'android tablet'"
        + "when dev.category is not null                                  then dev.category"
        + "else 'unknown'"
        + "end,"
        + "pst.day"
    );
    String dbName, tableOnlyName;
    dbName = "default";
    tableOnlyName = "dgies_tmp_pst_dev";
    TestTable tTable = TestTable.fromHiveMetastore(dbName, tableOnlyName);
    tableCompare(tTable);
  }
  
  @Test
  public void q2() throws Exception {
    set("hive.auto.convert.join", "true");
    set("hive.exec.dynamic.partition", "true");
    set("hive.exec.dynamic.partition.mode", "nonstrict");
    set("mapreduce.map.java.opts", "-xms4096m -xmx4096m");
    set("mapreduce.map.memory.mb", "4096");
    set("hive.exec.max.dynamic.partitions", "2000");
    set("hive.hadoop.supports.splittable.combineinputformat", "true");
    set("hive.input.format", "org.apache.hadoop.hive.ql.io.combinehiveinputformat");
    set("mapreduce.input.fileinputformat.split.maxsize", "256000000");
    set("hive.exec.max.dynamic.partitions.pernode", "1000");
    runQuery(
        "insert into table dgies_delivered_oline_listener_device_yob_geos partition (active, day)"
        + "select /*+ mapjoin(dgies_delivery_tracking_delta) */"
        + "d.dfp_advertiser_id,"
        + "d.dfp_order_id,"
        + "d.dfp_line_item_id,"
        + "d.dfp_creative_id,"
        + "d.order_id,"
        + "d.oline_id,"
        + "d.flight_id,"
        + "d.listener_id,"
        + "d.country_code,"
        + "d.zip,"
        + "d.birth_year,"
        + "d.gender,"
        + "d.device_category,"
        + "d.is_demo_on_target,"
        + "d.is_geo_on_target,"
        + "d.is_date_in_range,"
        + "d.impressions,"
        + "d.clicks,"
        + "d.network_impressions,"
        + "d.network_clicks,"
        + "case"
        + "when ("
        + "d.active in ('order', 'line')"
        + "and ("
        + "("
        + "track.late_effective_end_date"
        + "and track.late_order_latest_delivery"
        + ")"
        + "and track.has_effective_end_date"
        + ")"
        + ") then 'inactive'"
        + "when ("
        + "d.active = 'line'"
        + "and ("
        + "("
        + "track.late_oline_end_date"
        + "and track.late_oline_latest_delivery"
        + ")"
        + "and track.has_oline_end_date"
        + ")"
        + ") then 'order'"
        + "end as active,"
        + "d.day"
        + "from dgies_delivered_oline_listener_device_yob_geos d"
        + "left outer join dgies_delivery_tracking_delta track"
        + "on concat(d.order_id, '-', d.oline_id) = concat(track.order_id, '-', track.oline_id)"
        + "where d.active in ('order', 'line')"
        + "and"
        + "("
        + "("
        + "d.active in ('order', 'line')"
        + "and ("
        + "("
        + "track.late_effective_end_date"
        + "and track.late_order_latest_delivery"
        + ")"
        + "and track.has_effective_end_date"
        + ")"
        + ")"
        + "or"
        + "("
        + "d.active = 'line'"
        + "and ("
        + "("
        + "track.late_oline_end_date"
        + "and track.late_oline_latest_delivery"
        + ")"
        + "and track.has_oline_end_date"
        + ")"
        + ")"
        + ")"
    );
    String dbName, tableOnlyName;
    dbName = "default";
    tableOnlyName = "dgies_delivered_oline_listener_device_yob_geos";
    TestTable tTable = TestTable.fromHiveMetastore(dbName, tableOnlyName);
    tableCompare(tTable);
  }
  
  @Test
  public void q1() throws Exception {
    set("hive.hadoop.supports.splittable.combineinputformat", "false");
    set("mapreduce.input.fileinputformat.split.maxsize", "5000000");
    set("mapred.max.split.size", "5000000");
    runQuery(
        "drop table if exists dgies_cp_targets"
    );
    runQuery(
        "create table dgies_cp_targets ("
        + "olf            string,"
        + "oline_id       bigint,"
        + "flight_id      bigint,"
        + "updated_at     string,"
        + "num_demo       int,"
        + "demo_target    string,"
        + "num_geo        int,"
        + "geo_target     string"
        + ")"
        + "stored as orcfile"
    );
    set("mapred.reduce.tasks", "10");
    runQuery(
        "insert overwrite table dgies_cp_targets"
        + "select"
        + "collected.olf,"
        + "collected.oline_id,"
        + "collected.flight_id,"
        + "collected.updated_at,"
        + "collected.num_demo,"
        + "collected.demo_target,"
        + "collected.num_geo,"
        + "trim(concat_ws(' ',"
        + "countries.country_target,"
        + "collected.geo_target"
        + ")) as geo_target"
        + "from"
        + "("
        + "select"
        + "concat(cast(oline_id as string), '-', cast(coalesce(flight_id, 0) as string)) as olf,"
        + "oline_id,"
        + "flight_id,"
        + "updated_at,"
        + "sum(if(demo_target != '', 1, 0))         as num_demo,"
        + "trim(concat_ws(' ', sort_array(collect_list(demo_target)))) as demo_target,"
        + "sum(if(geo_target != '', 1, 0))          as num_geo,"
        + "trim(concat_ws(' ', sort_array(collect_list(geo_target )))) as geo_target"
        + "from dgies_cp_targ_map"
        + "group by"
        + "concat(cast(oline_id as string), '-', cast(coalesce(flight_id, 0) as string)),"
        + "oline_id,"
        + "flight_id,"
        + "updated_at"
        + ") collected"
        + "left outer join dgies_olines_country_targets countries"
        + "on collected.oline_id = countries.oline_id"
    );
    String dbName, tableOnlyName;
    dbName = "default";
    tableOnlyName = "dgies_cp_targets";
    TestTable tTable = TestTable.fromHiveMetastore(dbName, tableOnlyName);
    tableCompare(tTable);
  }
  
  @Test
  public void q4() throws Exception {
    set("hive.auto.convert.join", "true");
    runQuery(
        "drop table if exists dgies_geographies"
    );
    runQuery(
        "create table dgies_geographies ("
        + "country_code             string,"
        + "postal_code              string,"
        + "dma_id                   string,"
        + "dma_name                 string,"
        + "msa_id                   string,"
        + "msa_name                 string,"
        + "msa_duplicate           boolean,"
        + "county_id                string,"
        + "county_name              string,"
        + "state_name               string,"
        + "congressional_district   string,"
        + "congressional_duplicate boolean,"
        + "state_upper              string,"
        + "upper_duplicate         boolean,"
        + "state_lower              string,"
        + "lower_duplicate         boolean,"
        + "au_state                 string,"
        + "nz_region                string,"
        + "au_market_id             string,"
        + "au_market_name           string,"
        + ")"
        + "stored as orcfile"
    );
    runQuery(
        "insert overwrite table dgies_geographies"
        + "select /*+ mapjoin(analytics_lds_utils_zip_dma, analytics_lds_utils_zip_msa,  analytics_lds_utils_zip_county, analytics_lds_utils_zip_state) */"
        + "* from ("
        + ""
        + "select"
        + "'us' as country_code,"
        + "coalesce(county.zip, state.zip, dma.zip, msa.zip, federal.zip) as postal_code,"
        + "dma.dma_id                 as dma_id,"
        + "dma.dma                    as dma_name,"
        + "msa.msa_id                 as msa_id,"
        + "msa.msa_name               as msa_name,"
        + "coalesce(msa.duplicate, false)              as msa_duplicate,"
        + "county.county_id           as county_id,"
        + "county.county              as county_name,"
        + "state.state                as state_name,"
        + "federal.district           as congressional_district,"
        + "coalesce(federal.duplicate, false)          as congressional_duplicate,"
        + "st_upper.district          as state_upper,"
        + "coalesce(st_upper.duplicate, false)         as upper_duplicate,"
        + "st_lower.district          as state_lower,"
        + "coalesce(st_lower.duplicate, false)         as lower_duplicate,"
        + "null                       as au_state,"
        + "null                       as nz_region,"
        + "null                       as au_market_id,"
        + "null                       as au_market_name"
        + "from            analytics_lds_utils_zip_county county"
        + "full outer join analytics_lds_utils_zip_state  state   on state.zip = county.zip"
        + "full outer join analytics_lds_utils_zip_dma    dma     on dma.zip   = coalesce(county.zip, state.zip)"
        + "full outer join ("
        + "select zip, msa_id, msa_name,"
        + "if(rank() over (partition by zip order by msa_id) > 1, true, false) as duplicate"
        + "from analytics_lds_utils_zip_msa"
        + ") msa"
        + "on msa.zip = coalesce(county.zip, state.zip, dma.zip)"
        + "full outer join ("
        + "select zip, concat(state, '-', district) as district,"
        + "if(rank() over (partition by zip order by district) > 1, true, false) as duplicate"
        + "from analytics_lds_utils_zip_federal_congressional_districts"
        + ") federal"
        + "on federal.zip = coalesce(county.zip, state.zip, dma.zip, msa.zip)"
        + "and coalesce(msa.duplicate, false) = false"
        + "full outer join ("
        + "select zip, concat(state, '-', district, '-', upper_lower) as district, if(rank() over (partition by zip order by state, district, upper_lower) > 1, true, false) as duplicate"
        + "from analytics_lds_utils_zip_state_legislative_districts where upper_lower = 'u'"
        + ") st_upper"
        + "on st_upper.zip = coalesce(county.zip, state.zip, dma.zip, msa.zip, federal.zip)"
        + "and coalesce(msa.duplicate, false) = false"
        + "and coalesce(federal.duplicate, false) = false"
        + "full outer join ("
        + "select zip, concat(state, '-', district, '-', upper_lower) as district, if(rank() over (partition by zip order by state, district, upper_lower) > 1, true, false) as duplicate"
        + "from analytics_lds_utils_zip_state_legislative_districts where upper_lower = 'l'"
        + ") st_lower"
        + "on st_lower.zip = coalesce(county.zip, state.zip, dma.zip, msa.zip, federal.zip, st_upper.zip)"
        + "and coalesce(msa.duplicate, false) = false"
        + "and coalesce(federal.duplicate, false) = false"
        + "and coalesce(st_upper.duplicate, false) = false"
        + ""
        + "union all"
        + "select"
        + "country_code,"
        + "postal_code,"
        + "null        as dma_id,"
        + "null        as dma_name,"
        + "null        as msa_id,"
        + "null        as msa_name,"
        + "false       as msa_duplicate,"
        + "null        as county_id,"
        + "null        as county_name,"
        + "null        as state_name,"
        + "null        as congressional_district,"
        + "false       as congressional_duplicate,"
        + "null as state_upper,"
        + "false as upper_duplicate,"
        + "null as state_lower,"
        + "false as lower_duplicate,"
        + "if(country_code = 'au', region_code, null) as au_state,"
        + "if(country_code = 'nz', region_code, null) as nz_region,"
        + "if(country_code = 'au', market_code, null) as au_market_id,"
        + ""
        + "if(country_code = 'au', trim(regexp_replace(market_name, 'tv1', '')), null) as au_market_name"
        + "from analytics_au_nz_geo_pop_120513"
        + ") u "
    );
    runQuery(
        "drop table if exists dgies_geo_rollup"
    );
    runQuery(
        "create table dgies_geo_rollup ("
        + "country_code            string,"
        + "postal_code             string,"
        + "dma_ids                 string,"
        + "msa_ids                 string,"
        + "county_ids              string,"
        + "state_names             string,"
        + "congressional_districts string,"
        + "state_districts         string,"
        + "au_state_names          string,"
        + "nz_region_names         string,"
        + "au_market_ids           string,"
        + ")"
        + "stored as orcfile"
    );
    runQuery(
        "insert overwrite table dgies_geo_rollup"
        + "select"
        + "country_code,"
        + "postal_code,"
        + "concat_ws('|', collect_set(dma_id))                 as dma_ids,"
        + "concat_ws('|', collect_set(msa_id))                 as msa_ids,"
        + "concat_ws('|', collect_set(county_id))              as county_ids,"
        + "concat_ws('|', collect_set(state_name))             as state_names,"
        + "concat_ws('|', collect_set(congressional_district)) as congressional_districts,"
        + "case"
        + "when concat_ws('|', collect_set(state_upper), collect_set(state_lower)) not in ('', '|')"
        + "then concat_ws('|', collect_set(state_upper), collect_set(state_lower))"
        + "else null"
        + "end as state_districts,"
        + "concat_ws('|', collect_set(au_state))               as au_state_names,"
        + "concat_ws('|', collect_set(nz_region))              as nz_region_names,"
        + "concat_ws('|', collect_set(au_market_id))           as au_market_ids"
        + "from dgies_geographies"
        + "group by"
        + "country_code,"
        + "postal_code "
    );
    String dbName, tableOnlyName;
    dbName = "default";
    tableOnlyName = "dgies_geo_rollup";
    TestTable tTable = TestTable.fromHiveMetastore(dbName, tableOnlyName);
    tableCompare(tTable);
  }
  
  @Test
  public void q3() throws Exception {
    set("hive.auto.convert.join", "true");
    set("hive.exec.dynamic.partition", "true");
    set("hive.exec.dynamic.partition.mode", "nonstrict");
    set("mapreduce.map.java.opts", "-xms4096m -xmx4096m");
    set("mapreduce.map.memory.mb", "4096");
    set("hive.exec.max.dynamic.partitions", "2000");
    set("hive.hadoop.supports.splittable.combineinputformat", "true");
    set("hive.input.format", "org.apache.hadoop.hive.ql.io.combinehiveinputformat");
    set("mapreduce.input.fileinputformat.split.maxsize", "256000000");
    set("hive.exec.max.dynamic.partitions.pernode", "1000");
    runQuery(
        "insert overwrite table dgies_delivered_oline_listener_device_yob_geos partition (active, day)"
        + "select /*+ mapjoin(dgies_delivery_tracking_delta) */"
        + "d.dfp_advertiser_id,"
        + "d.dfp_order_id,"
        + "d.dfp_line_item_id,"
        + "d.dfp_creative_id,"
        + "d.order_id,"
        + "d.oline_id,"
        + "d.flight_id,"
        + "d.listener_id,"
        + "d.country_code,"
        + "d.zip,"
        + "d.birth_year,"
        + "d.gender,"
        + "d.device_category,"
        + "d.is_demo_on_target,"
        + "d.is_geo_on_target,"
        + "d.is_date_in_range,"
        + "d.impressions,"
        + "d.clicks,"
        + "d.network_impressions,"
        + "d.network_clicks,"
        + "case"
        + "when ("
        + "d.active = 'line'"
        + "and ("
        + "not track.late_oline_end_date"
        + "or not track.late_oline_latest_delivery"
        + "or not track.has_oline_end_date"
        + ")"
        + ") then 'line'"
        + "when ("
        + "d.active = 'order'"
        + "and ("
        + "not track.late_effective_end_date"
        + "or not track.late_order_latest_delivery"
        + "or not track.has_effective_end_date"
        + ")"
        + ") then 'order'"
        + "end as active,"
        + "d.day"
        + "from dgies_delivered_oline_listener_device_yob_geos d"
        + "left outer join dgies_delivery_tracking_delta track"
        + "on concat(d.order_id, '-', d.oline_id) = concat(track.order_id, '-', track.oline_id)"
        + "where d.active in ('order', 'line')"
        + "and ("
        + "("
        + "d.active = 'line'"
        + "and ("
        + "not track.late_oline_end_date"
        + "or not track.late_oline_latest_delivery"
        + "or not track.has_oline_end_date"
        + ")"
        + ")"
        + "or"
        + "("
        + "d.active = 'order'"
        + "and ("
        + "not track.late_effective_end_date"
        + "or not track.late_order_latest_delivery"
        + "or not track.has_effective_end_date"
        + ")"
        + ")"
        + ")"
    );
    String dbName, tableOnlyName;
    dbName = "default";
    tableOnlyName = "dgies_delivered_oline_listener_device_yob_geos";
    TestTable tTable = TestTable.fromHiveMetastore(dbName, tableOnlyName);
    tableCompare(tTable);
  }
  
}
