/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.test.capybara.infra;

import org.apache.hadoop.hive.common.ObjectPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A class to transform Hive SQL to the SQL dialect of the benchmark.  This base class handles
 * the most common issues, but each type of benchmark will need to extend this to handle quirks
 * of its own SQL dialect.  The class is built to be mostly stateless so that {@link #translate}
 * can be called repeatedly with different SQL statements on the same object.  The one bit of
 * state kept is when "use database" is translated, so that the current database is tracked.
 */
abstract class SQLTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(SQLTranslator.class.getName());

  protected static final String ID_REGEX = "[a-zA-Z0-9_]+";
  protected static final String TABLE_NAME_REGEX = "(?:" + ID_REGEX + "\\.)?" + ID_REGEX;
  protected static final String QUOTE_START = "CQ_";
  protected static final String QUOTE_END = "_QC";
  protected static final String QUOTE_REGEX = QUOTE_START + "[0-9]+" + QUOTE_END;
  protected static final String NUMERIC_CONSTANT_REGEX = "([0-9]+)(l|s|y|bd)";


  /**
   * If true, it's ok if this SQL fails to run on the benchmark.  This is used to mask the fact
   * that the database may not have 'if not exists' or 'if exists' which can obviously cause
   * failures.
   */
  protected boolean failureOk;

  /**
   * Current database, used in table names
   */
  protected String currentDb = "default";

  /**
   * Translate the Hive SQL to the appropraite dialect.  This call breaks up the Hive SQL into
   * separate sections.  It is final because sub-classes shouldn't change the way it is broken up.
   * @param hiveSql SQL in Hive dialect
   * @return SQL in benchmark dialect.
   */
  final String translate(String hiveSql) throws TranslationException {
    failureOk = false;
    String trimmed = hiveSql.trim().toLowerCase();

    // Remove any hints
    Matcher matcher = Pattern.compile("/\\*.*\\*/").matcher(trimmed);
    trimmed = matcher.replaceAll("");

    trimmed = deQuote(trimmed);
    // Convert all white space to single spaces so we don't have to keep matching \\s+ everywhere
    Matcher m = Pattern.compile("\\s+").matcher(trimmed);
    trimmed = m.replaceAll(" ");

    String benchSql;
    if (Pattern.compile("select").matcher(trimmed).lookingAt()) {
      benchSql = translateSelect(trimmed);
    } else if (Pattern.compile("insert").matcher(trimmed).lookingAt()) {
      benchSql = translateInsert(trimmed);
    } else if (Pattern.compile("explain").matcher(trimmed).lookingAt()) {
      benchSql = "";
    } else if (Pattern.compile("update").matcher(trimmed).lookingAt()) {
      benchSql = translateUpdate(trimmed);
    } else if (Pattern.compile("delete from").matcher(trimmed).lookingAt()) {
      benchSql = translateDelete(trimmed);
    } else if (Pattern.compile("create (temporary |external )?table").matcher(trimmed).lookingAt()) {
      benchSql = translateCreateTable(trimmed);
    } else if (Pattern.compile("drop table").matcher(trimmed).lookingAt()) {
      benchSql = translateDropTable(trimmed);
    } else if (Pattern.compile("alter table").matcher(trimmed).lookingAt()) {
      benchSql = translateAlterTable(trimmed);
    } else if (Pattern.compile("msck (repair )?table").matcher(trimmed).lookingAt()) {
      benchSql = "";
    } else if (Pattern.compile("show").matcher(trimmed).lookingAt()) {
      benchSql = "";
    } else if (Pattern.compile("describe").matcher(trimmed).lookingAt()) {
      benchSql = "";
    } else if (Pattern.compile("create (database|schema)").matcher(trimmed).lookingAt()) {
      benchSql = translateCreateDatabase(trimmed);
    } else if (Pattern.compile("alter (database|schema)").matcher(trimmed).lookingAt()) {
      benchSql = "";
    } else if (Pattern.compile("drop (database|schema)").matcher(trimmed).lookingAt()) {
      benchSql = translateDropDatabase(trimmed);
    } else if (Pattern.compile("use (" + ID_REGEX +")").matcher(trimmed).lookingAt()) {
      benchSql = translateUseDatabase(m.group(1));
    } else if (Pattern.compile("analyze").matcher(trimmed).lookingAt()) {
      benchSql = "";
    } else if (Pattern.compile("create role").matcher(trimmed).lookingAt()) {
      benchSql = "";
    } else if (Pattern.compile("drop role").matcher(trimmed).lookingAt()) {
      benchSql = "";
    } else if (Pattern.compile("set role").matcher(trimmed).lookingAt()) {
      benchSql = "";
    } else if (Pattern.compile("grant").matcher(trimmed).lookingAt()) {
      benchSql = "";
    } else if (Pattern.compile("revoke").matcher(trimmed).lookingAt()) {
      benchSql = "";
    } else if (Pattern.compile("create index").matcher(trimmed).lookingAt()) {
      benchSql = "";
    } else if (Pattern.compile("alter index").matcher(trimmed).lookingAt()) {
      benchSql = "";
    } else if (Pattern.compile("drop index").matcher(trimmed).lookingAt()) {
      benchSql = "";
    } else if (Pattern.compile("create (temporary )?function").matcher(trimmed).lookingAt()) {
      // This won't end well.  We can't translate functions
      throw new TranslationException("create function", hiveSql);
    } else if (Pattern.compile("drop (temporary )?function").matcher(trimmed).lookingAt()) {
      throw new TranslationException("drop function", hiveSql);
    } else if (Pattern.compile("reload function").matcher(trimmed).lookingAt()) {
      throw new TranslationException("reload function", hiveSql);
    } else {
      throw new TranslationException("Unrecognized", hiveSql);
    }
    return reQuote(benchSql);

    // TODO:
    // alter view
    // create view
    // drop view
    // load
    // truncate table - have to handle truncate partition and weird partition casts
    // with
  }

  public boolean isFailureOk() {
    return failureOk;
  }

  /**********************************************************************************************
   * DB related
   **********************************************************************************************/
  /**
   * Translate create database.
   * @param hiveSql hiveSql, already trimmed and coverted to lower case.
   * @return benchmark sql
   */
  protected String translateCreateDatabase(String hiveSql) throws
      TranslationException {
    // Convert 'create database' to 'create schema'
    Matcher m = Pattern.compile("create database").matcher(hiveSql);
    String benchSql = m.replaceAll("create schema");

    // Remove any other modifiers (location, comment, dbproperties)...
    //m = Pattern.compile("create schema (?:if not exists )?([a-zA-Z0-9][a-zA-Z0-9_]*) .*").matcher(benchSql);
    m = Pattern.compile("create schema (if not exists )?(" + ID_REGEX + ")(?: .*)?").matcher(
        benchSql);
    if (m.lookingAt()) {
      return "create schema " + (m.group(1) == null ? "" : m.group(1)) + m.group(2);
    }

    throw new TranslationException("create database", hiveSql);
  }

  protected String translateDropDatabase(String hiveSql) throws TranslationException {
    // Convert 'drop database' to 'create schema'
    Matcher m = Pattern.compile("drop database").matcher(hiveSql);
    return m.replaceAll("drop schema");
  }

  protected String translateUseDatabase(String dbName) throws TranslationException {
    // Find the database name and set that as the default.
    currentDb = dbName;
    // Use database doesn't translate.
    return "";
  }

  /**********************************************************************************************
   * Table related
   **********************************************************************************************/
  private String translateCreateTable(String hiveSql) throws TranslationException {

    // Look for like
    Matcher m = Pattern.compile("create (temporary |external )?table (if not exists )?(" +
        TABLE_NAME_REGEX + ") like (" + TABLE_NAME_REGEX + ")").matcher(hiveSql);
    if (m.lookingAt()) return translateCreateTableLike(m);

    // Look for as
    m = Pattern.compile("create (temporary |external )?table (if not exists )?(" +
        TABLE_NAME_REGEX + ") (?:.* )?as (select.*)").matcher(hiveSql);
    if (m.lookingAt()) return translateCreateTableAs(m);

    // Must be your basic create table foo (x int ...) type
    m = Pattern.compile("create (temporary |external )?table (if not exists )?(" +
        TABLE_NAME_REGEX + ") ?\\((.*)").matcher(hiveSql);
    if (m.lookingAt()) return translateCreateTableWithColDefs(m);

    throw new TranslationException("create table", hiveSql);
  }

  /**
   * Translate a create table like
   * @param matcher A matcher for the statement,
   *                group 1 is temporary or external (may be null),
   *                group 2 is 'if not exists', (may be null)
   *                group 3 is the new table name
   *                group 4 is the source table name
   * @return translated SQL
   */
  protected String translateCreateTableLike(Matcher matcher) {
    StringBuilder sql = new StringBuilder("create ");
    if (matcher.group(1) != null && matcher.group(1).equals("temporary ")) sql.append("temporary ");
    sql.append("table ");
    if (matcher.group(2) != null) sql.append(matcher.group(2));
    sql.append(matcher.group(3))
        .append(" like ")
        .append(matcher.group(4));
    return sql.toString();
  }

  /**
   * Translate a create table as
   * @param matcher A matcher for the statement
   *                group 1 is temporary or external (may be null)
   *                group 2 is 'if not exists', may be null
   *                group 3 is the table name
   *                group 4 is the select query
   * @return translated SQL
   */
  protected String translateCreateTableAs(Matcher matcher) throws TranslationException {
    StringBuilder sql = new StringBuilder("create ");
    if (matcher.group(1) != null && matcher.group(1).equals("temporary ")) sql.append("temporary ");
    sql.append("table ");
    if (matcher.group(2) != null) sql.append(matcher.group(2));
    sql.append(matcher.group(3))
        .append(" as ")
        .append(translateSelect(matcher.group(4)));
    return sql.toString();
  }

  /**
   * Translate create table iwth column definitions
   * @param matcher A matcher for the statement
   *                group 1 is temporary or external (may be null)
   *                group 2 is 'if not exists', may be null
   *                group 3 is the table name
   *                group 4 column definitions and the rest of the query
   * @return bench sql
   */
  protected String translateCreateTableWithColDefs(Matcher matcher) {
    StringBuilder sql = new StringBuilder("create ");
    if (matcher.group(1) != null && matcher.group(1).equals("temporary ")) sql.append("temporary ");
    sql.append("table ");
    if (matcher.group(2) != null) sql.append(matcher.group(2));
    sql.append(matcher.group(3))
        .append(" (")
        .append(translateDataTypes(parseOutColDefs(matcher.group(4))));
    return sql.toString();
  }

  protected String parseOutColDefs(String restOfQuery) {
    StringBuilder cols = new StringBuilder();
    int level = 1;
    for (int i = 0; i < restOfQuery.length() && level > 0; i++) {
      char current = restOfQuery.charAt(i);
      cols.append(current);
      if (current == '(') level++;
      else if (current == ')') level--;
    }
    return cols.toString();
  }

  /**
   * Translate data types
   * @param hiveSql hive data types, may contain other text as well
   * @return transformed text, with datatypes changed and other contents untouched.
   */
  protected abstract String translateDataTypes(String hiveSql);

  protected String translateDropTable(String hiveSql) throws TranslationException {
    // Need to remove purge if its there
    Matcher m = Pattern.compile("drop table (if exists )?(" + TABLE_NAME_REGEX + ")").matcher(hiveSql);
    if (m.lookingAt()) {
      StringBuilder sql = new StringBuilder("drop table ");
      if (m.group(1) != null) sql.append(m.group(1));
      sql.append(m.group(2));
      return sql.toString();
    } else {
      throw new TranslationException("drop table", hiveSql);
    }
  }

  private String translateAlterTable(String hiveSql) throws TranslationException {
    // It has been said that enough monkeys pounding on typewriters would eventually produce the
    // complete works of Shakespeare.  In the case of Hive's alter table syntax someone gave the
    // monkeys whiskey first.
    Matcher m = Pattern.compile("alter table (" + TABLE_NAME_REGEX + ") ").matcher(hiveSql);
    if (m.lookingAt()) {
      String tableName = translateTableNames(m.group(1));
      String remainder = hiveSql.substring(m.end());

      // See if this has a partition clause after the table name (though some types put it after
      // the verb, see what I meant about the whiskey?).  If it does, parse through that and
      // remember the values in case we need them.
      PartitionClause partition = null;
      if (remainder.startsWith("partition")) {
        partition = parsePartition(remainder);
        remainder = remainder.substring(partition.length).trim();
      }

      if (remainder.startsWith("rename to")) {
        if (partition == null) {
          return translateAlterTableRename(tableName, remainder);
        } else {
          // TODO - once we can parse out partition values we can support this, as it's just an
          // update.
          LOG.error("alter table partition rename not yet supported");
          throw new TranslationException("alter table partition rename", hiveSql);
        }
      }
      if (remainder.startsWith("set")) return "";
      if (remainder.startsWith("clustered by")) return "";
      if (remainder.startsWith("skewed by")) return "";
      if (remainder.startsWith("not skewed")) return "";
      if (remainder.startsWith("not stored as directories")) return "";
      if (remainder.startsWith("add partition") ||
          remainder.startsWith("add if not exists partition")) {
        return "";
      }
      if (remainder.startsWith("drop partition") ||
          remainder.startsWith("drop if exists partition")) {
        return "";
      }
      if (remainder.startsWith("exchange")) {
        LOG.error("alter table exchange partition not supported");
        throw new TranslationException("alter table exchange", hiveSql);
      }
      if (remainder.startsWith("archive")) return "";
      if (remainder.startsWith("unarchive")) return "";
      if (remainder.startsWith("touch")) return "";
      if (remainder.startsWith("enable")) return "";
      if (remainder.startsWith("disable")) return "";
      if (remainder.startsWith("compact")) return "";
      if (remainder.startsWith("concatenate")) return "";
      if (remainder.startsWith("change")) {
        LOG.error("alter table change column not supported, as changing data types is seriously " +
            "dicey and we can't tell whether the datatype is being changed or not.");
        throw new TranslationException("alter table change column", hiveSql);
      }

      throw new TranslationException("unknown alter table", hiveSql);

    } else {
      throw new TranslationException("alter table", hiveSql);
    }

  }

  private class PartitionClause {
    final int length;
    final List<ObjectPair<String, String>> partKeyVals; // val might be null

    public PartitionClause(int length, List<ObjectPair<String, String>> partKeyVals) {
      this.length = length;
      this.partKeyVals = partKeyVals;
    }
  }

  private PartitionClause parsePartition(String hiveSql) throws TranslationException {
    // Find the end of the partition clause.  We've masked out the quotes so there shouldn't be
    // any close parends left.
    int endPartition = hiveSql.indexOf(')');
    if (endPartition == -1) {
      throw new TranslationException("partition", hiveSql);
    }

    // Run the whole thing through the constant translator
    String benchSql = translateConstants(hiveSql.substring(0, endPartition + 1));

    int length = 0;
    List<ObjectPair<String, String>> partKeyValues = new ArrayList<>();
    Matcher m = Pattern.compile("(partition ?\\()").matcher(benchSql);
    // I can just read until the next comma or close parend because I've already turned the
    // quotes into identifiers to avoid issues.
    Pattern keyValue = Pattern.compile("(" + ID_REGEX + ") ?=(.+?)[,)]");
    Pattern key = Pattern.compile(ID_REGEX);
    if (m.lookingAt()) {
      length += m.group().length();

      while (true) {
        if (benchSql.charAt(length) == ' ') length++;

        // The next thing we see should be either an key = value or just key
        Matcher kvm = keyValue.matcher(benchSql.substring(length));
        if (kvm.lookingAt()) {
          length += kvm.group().length() - 1; // subtract one to get rid of the , or )
          partKeyValues.add(new ObjectPair<>(kvm.group(1), kvm.group(2).trim()));
        } else {
          Matcher km = key.matcher(benchSql.substring(length));
          if (km.lookingAt()) {
            length += km.group().length();
            // This is just the key, so add just the key to the map
            partKeyValues.add(new ObjectPair<>(km.group(), (String)null));
          } else {
            LOG.error("Expected to find 'partkey' or 'partkey = partval', but instead found: " +
              benchSql.substring(length));
            throw new TranslationException("partition", hiveSql);
          }
        }

        // Okay, now we might see a comma next or a ).  If it's a comma we need to move length
        // forward and do it again.  If it's ) we're done, time to return.
        if (benchSql.charAt(length) == ' ') length++;
        if (benchSql.charAt(length) == ')') {
          return new PartitionClause(endPartition + 1,  partKeyValues);
        } else if (benchSql.charAt(length) == ',') {
          length++;
        } else {
          LOG.error("Expected to find ')' or ',' but instead found " + benchSql.charAt(length));
          throw new TranslationException("partition", hiveSql);
        }
      }
    }
    throw new TranslationException("partition", hiveSql);
  }

  /**
   * Translate alter table rename
   * @param tableName translated table name
   * @param remainder remainder of the Hive SQL, commencing after the table name.  It should
   *                  start with 'rename to'
   * @return translated SQL, this should include the entire SQL, not just the remainder (ie it
   * should start with 'alter table'
   * @throws TranslationException
   */
  protected abstract String translateAlterTableRename(String tableName, String remainder)
    throws TranslationException;

  /**********************************************************************************************
   * Query related
   **********************************************************************************************/
  final protected String translateSelect(String hiveSql) throws TranslationException {
    return translateSelect(hiveSql, null);
  }

  /**
   *
   * @param hiveSql SQL from Hive
   * @param extraProjections A set of key value pairs (picked up from a partition clause) that
   *                         need to be added to the projection list of this select.  Some of the
   *                         keys may have null values, which means they are already accounted
   *                         for and can be ignored.
   * @return
   * @throws TranslationException
   */
  private String translateSelect(String hiveSql, List<ObjectPair<String, String>> extraProjections)
      throws TranslationException {
    StringBuilder benchSql = new StringBuilder("select ");
    int current = 7; // pointer to our place in the SQL stream

    // Handle all|distinct
    if (hiveSql.substring(current).startsWith("all ")) {
      benchSql.append("all ");
      current += 4;
    } else if (hiveSql.substring(current).startsWith("distinct ")) {
      benchSql.append("distinct ");
      current+= 9;
    }

    // Move past the projection list
    int nextKeyword = findNextKeyword(hiveSql, current, Arrays.asList(" from ", " where ",
        " group by ", " having ", " union ", " order by ", " limit "));
    benchSql.append(translateExpressions(hiveSql.substring(current, nextKeyword)));
    if (extraProjections != null) {
      benchSql.append(appendPartitionProjection(hiveSql, extraProjections));
    }
    current = nextKeyword;

    // Handle a from if it's there
    if (current < hiveSql.length() && hiveSql.substring(current).startsWith(" from ")) {
      // We can't just look for the next keyword, as there can be subqueries in the from clause,
      // so we need to handle any parenthesis
      current += 6; // move past " from "
      nextKeyword = findNextKeyword(hiveSql, current, Arrays.asList(" where ", " group by ",
          " having ", " union ", " order by ", " limit "));
      benchSql.append(" from ")
          .append(translateFrom(hiveSql.substring(current, nextKeyword)));
      current = nextKeyword;
    }

    // We may be on where
    if (current < hiveSql.length() && hiveSql.substring(current).startsWith(" where ")) {
      // Again, we have to be aware of parenthesis
      current += 7; // move past " where "
      nextKeyword = findNextKeyword(hiveSql, current, Arrays.asList(" group by ", " having ",
          " union ", " order by ", " limit "));
      benchSql.append(" where ")
          .append(translateExpressions(hiveSql.substring(current, nextKeyword)));
      current = nextKeyword;
    }

    // Now we might be on group by, shouldn't be anything to translate here
    if (current < hiveSql.length() && hiveSql.substring(current).startsWith(" group by ")) {
      current += 10; // move past " group by "
      nextKeyword = findNextKeyword(hiveSql, current, Arrays.asList(" having ", " union ",
          " order by ", " limit "));
      benchSql.append(" group by ")
          .append(hiveSql.substring(current, nextKeyword));
      current = nextKeyword;
    }

    // Maybe having
    if (current < hiveSql.length() && hiveSql.substring(current).startsWith(" having ")) {
      // Again, we have to be aware of parenthesis
      current += 8; // move past " having "
      nextKeyword = findNextKeyword(hiveSql, current, Arrays.asList(" union ", " order by ",
          " limit "));
      benchSql.append(" having ")
          .append(translateExpressions(hiveSql.substring(current, nextKeyword)));
      current = nextKeyword;
    }

    // Maybe union
    if (current < hiveSql.length() && hiveSql.substring(current).startsWith(" union ")) {
      // Again, we have to be aware of parenthesis
      current += 7; // move past " union "
      benchSql.append(" union ");
      if (hiveSql.substring(current).startsWith("all ")) {
        benchSql.append("all ");
        current += 4;
      } else if (hiveSql.substring(current).startsWith("distinct ")) {
        benchSql.append("distinct ");
        current += 9;
      }
      benchSql.append(translateSelect(hiveSql.substring(current)));
      // Any trailing orders or limits will get attached to the last select, so just return here.
      return benchSql.toString();
    }

    // Maybe order by, shouldn't be anything to translate here
    if (current < hiveSql.length() && hiveSql.substring(current).startsWith(" order by ")) {
      current += 10; // move past " group by "
      nextKeyword = findNextKeyword(hiveSql, current, Arrays.asList(" limit "));
      benchSql.append(" order by ")
          .append(hiveSql.substring(current, nextKeyword));
      current = nextKeyword;
    }

    // Maybe limit, this has to be handled specially since some databases don't handle it
    if (current < hiveSql.length() && hiveSql.substring(current).startsWith(" limit ")) {
      benchSql.append(translateLimit(hiveSql.substring(current)));
    }

    return benchSql.toString();
  }

  private String appendPartitionProjection(String hiveSql,
                                           List<ObjectPair<String, String>> partKeyVals)
      throws TranslationException {
    // TODO For now we only support all dynamic or all static, trying to figure out how to mix and
    // match them is challenging.
    boolean allNull = true, allNotNull = true;
    for (ObjectPair<String, String> kvp : partKeyVals) {
      allNull &= kvp.getSecond() == null;
      allNotNull &= kvp.getSecond() != null;
    }
    if (!allNotNull && !allNull) {
      LOG.error("For now we require that all keys in a partition clause have values or none do.");
      throw new TranslationException("partition", hiveSql);
    }

    // TODO Also, we assume that users gave the partition specification in the same order as the
    // partition keys in the metadata.  If not, we'll have a problem.
    // Fixing this will require having metadata for the table we're translating.

    if (allNull) {
      // The values are already in the projection list, so nothing to do.
      return "";
    }

    StringBuilder sql = new StringBuilder();
    for (ObjectPair<String, String> kvp : partKeyVals) {
      sql.append(", ")
          .append(kvp.getSecond());
    }

    return sql.toString();
  }

  private int findNextKeyword(String hiveSql, int current, List<String> keywords) {
    int level = 0;
    for (int i = current; i < hiveSql.length(); i++) {
      if (hiveSql.charAt(i) == '(') {
        level++;
      } else if (hiveSql.charAt(i) == ')') {
        level--;
      } else if (level == 0) {
        // Check whether we've hit a keyword.  This is O(n^2), but I can't see a way to make it
        // better.
        for (String keyword : keywords) {
          if (hiveSql.substring(i).startsWith(keyword)) return i;
        }
      }
    }
    return hiveSql.length();
  }

  protected String translateLimit(String hiveSql) throws TranslationException {
    // Have to add in the limit here, because it wasn't
    return hiveSql;
  }

  private String translateExpressions(String hiveSql) throws TranslationException {
    String benchSql = translateConstants(hiveSql);
    benchSql = translateCasts(benchSql);
    benchSql = translateUDFs(benchSql);
    return translateSubqueries(benchSql);
  }

  protected String translateConstants(String hiveSql) throws TranslationException {
    //return hiveSql;
    if (hiveSql.contains(" interval ")) {
      LOG.error("Interval type not yet supported.");
      throw new TranslationException("interval", hiveSql);
    }
    // Remove the annotations Hive uses for bigint, etc.
    String benchSql = hiveSql;
    Pattern p = Pattern.compile(NUMERIC_CONSTANT_REGEX);
    Matcher m = p.matcher(benchSql);
    while (m.find()) {
      benchSql = m.replaceFirst(m.group(1)); // Get rid of the letter qualifier
      m = p.matcher(benchSql);
    }

    // Make sure all dates and timestamps have 2 digit months
    p = Pattern.compile("(date|timestamp) (" + QUOTE_REGEX + ")");
    Pattern qpm = Pattern.compile("([0-9]{4})-([0-9])-");
    Pattern qpd = Pattern.compile("([0-9]{4})-([0-9]{2})-([0-9])( .*)?");
    m = p.matcher(benchSql);
    int current = 0;
    while (m.find(current)) {
      // The quotes have been replaced for safety.  We have to go into the quote map and check
      // that this quote is ok
      Quote quote = quotes.get(m.group(2));
      // fix the month if we need to
      Matcher qm = qpm.matcher(quote.value);
      if (qm.find()) {
        quote.value = qm.replaceFirst(qm.group(1) + "-0" + qm.group(2) + "-");
      }

      // fix the day if we need to
      Matcher qd = qpd.matcher(quote.value);
      if (qd.matches()) {
        quote.value = qd.replaceFirst(qd.group(1) + "-" + qd.group(2) + "-0" + qd.group(3)
          + (qd.group(4) == null ? "" : qd.group(4)));
      }
      current = m.end();
    }

    // Make sure all dates and timestamps have 2 digit days
    return benchSql;
  }

  private String translateCasts(String hiveSql) throws TranslationException {
     // We need to look for data type conversions in casts
    StringBuilder benchSql = new StringBuilder();
    int current = 0;
    Matcher m = Pattern.compile("cast\\((.*?)\\)").matcher(hiveSql);
    while (m.find(current)) {
      if (m.start() - current > 0) benchSql.append(hiveSql.substring(current, m.start()));
      benchSql.append("cast (")
          .append(translateDataTypes(m.group(1)))
          .append(')');
      current = m.end();
    }

    if (current != 0) {
      benchSql.append(hiveSql.substring(current));
      return benchSql.toString();
    } else {
      return hiveSql;
    }
  }

  static Map<String, String> udfMapping = null;

  /**
   * A method to fill out the mapping of Hive UDF names to benchmark UDF names.  Overrides of
   * this should always call super.fillOutUdfMapping first as this one creates the object.
   */
  protected void fillOutUdfMapping() {
    udfMapping = new HashMap<>();
    // Entries that aren't UDFs but look like one to the regex matcher
    udfMapping.put("cast", "cast");
    udfMapping.put("char", "char");
    udfMapping.put("decimal", "decimal");
    udfMapping.put("exists", "exists ");
    udfMapping.put("in", "in ");
    udfMapping.put("on", "on");
    udfMapping.put("varchar", "varchar");

    // Actual UDFs
    udfMapping.put("acos", "acos");
    udfMapping.put("asin", "asin");
    udfMapping.put("atan", "atan");
    udfMapping.put("avg", "avg");
    udfMapping.put("cos", "cos");
    udfMapping.put("count", "count");
    udfMapping.put("length", "length");
    udfMapping.put("max", "max");
    udfMapping.put("min", "min");
    udfMapping.put("sin", "sin");
    udfMapping.put("substring", "substring");
    udfMapping.put("sum", "sum");
    udfMapping.put("tan", "tan");

    // TODO go through all the Hive UDFs.

  }

  private String translateUDFs(String hiveSql) throws TranslationException {
    if (udfMapping == null) fillOutUdfMapping();
    Matcher m = Pattern.compile("(" + ID_REGEX + ") ?\\(").matcher(hiveSql);
    StringBuilder benchSql = new StringBuilder();
    int current = 0;
    while (m.find(current)) {
      if (m.start() - current > 0) benchSql.append(hiveSql.substring(current, m.start()));
      benchSql.append(translateUDF(m.group(1)))
          .append('(');
      current = m.end();
    }
    // Pick up whatever is left after the last match.
    if (current != 0) {
      benchSql.append(hiveSql.substring(current));
      return benchSql.toString();
    } else {
      return hiveSql;
    }
  }

  private String translateUDF(String udfName) throws TranslationException {
    String benchName = udfMapping.get(udfName);
    if (benchName == null) {
      throw new TranslationException("UDF name translation", udfName);
    }
    return benchName;
  }

  private String translateFrom(String hiveSql) throws TranslationException {
    hiveSql = translateTableNames(hiveSql);
    return translateSubqueries(hiveSql);
  }

  protected String translateTableNames(String hiveSql) {
    // For now just strain out the default if it's there.  Eventually need to add non-default
    // name if it's been set.
    Matcher matcher = Pattern.compile("default\\.").matcher(hiveSql);
    return matcher.replaceAll("");
  }

  private String translateSubqueries(String hiveSql) throws TranslationException {
    StringBuilder sql = new StringBuilder();
    Matcher m = Pattern.compile("\\( ?select").matcher(hiveSql);
    int current = 0;
    while (m.find(current)) {
      int level = 1;
      for (int i = m.start() + 1; i < hiveSql.length() && level > 0; i++) {
        if (hiveSql.charAt(i) == '(') {
          level++;
        } else if (hiveSql.charAt(i) == ')' && --level == 0) {
          if (m.start() - current > 0) sql.append(hiveSql.substring(current, m.start()));
          sql.append('(')
              .append(translateSelect(hiveSql.substring(m.start() + 1, i).trim()));
          // Don't append the final ')', it will get picked up by the append of the next section
          // of the query.
          current = i;
        }
      }
    }
    // We still need to copy the last bit in.
    if (current > 0 && current < hiveSql.length()) sql.append(hiveSql.substring(current));
    if (sql.length() > 0) return sql.toString();
    else return hiveSql;
  }

  /**********************************************************************************************
   * DML releated
   **********************************************************************************************/
  protected String translateInsert(String hiveSql) throws TranslationException {
    StringBuilder sql = new StringBuilder();
    Matcher m = Pattern.compile("insert (?:overwrite )?(?:into )?(?:table )?(" + TABLE_NAME_REGEX +
        ") ?").matcher(hiveSql);
    if (m.lookingAt()) {
      sql.append("insert into ")
          .append(translateTableNames(m.group(1)))
          .append(' ');
      int current = m.end();
      PartitionClause partition = null;
      if (hiveSql.substring(current).startsWith("partition")) {
        partition = parsePartition(hiveSql.substring(current));
        current += partition.length;
      }

      // TODO handle column names listed in insert
      // We might have a select, or we might have a values
      String remaining = hiveSql.substring(current).trim();
      if (remaining.startsWith("values")) {
        if (partition != null) {
          String paritionAddition =
              appendPartitionProjection(remaining.substring(6).trim(), partition.partKeyVals);
          if (!paritionAddition.equals("")) {
            // We have to parse out each values clause so we can append these values to each clause
            Matcher pm = Pattern.compile("\\)").matcher(remaining);
            int pos = 0;
            while (pm.find(pos)) {
              sql.append(remaining.substring(pos, pm.start()))
                  .append(paritionAddition)
                  .append(')');
              pos = pm.end();
            }
            return sql.toString();
          }
        }
        // TODO handle appending additional values in 'values' when we need to
        sql.append(remaining);
      } else {
        sql.append(translateSelect(remaining, (partition == null) ? null : partition.partKeyVals));
      }
      return sql.toString();
    } else {
      throw new TranslationException("insert", hiveSql);
    }
  }

  protected String translateUpdate(String hiveSql) throws TranslationException {
    StringBuilder sql = new StringBuilder();
    Matcher m = Pattern.compile("update (" + TABLE_NAME_REGEX + ") set ").matcher(hiveSql);
    if (m.lookingAt()) {
      sql.append("update ")
          .append(translateTableNames(m.group(1)))
          .append(" set ")
          .append(translateExpressions(hiveSql.substring(m.end())));
      return sql.toString();
    } else {
      throw new TranslationException("update", hiveSql);
    }
  }

  protected String translateDelete(String hiveSql) throws TranslationException {
    StringBuilder sql = new StringBuilder();
    Matcher m = Pattern.compile("delete from (" + TABLE_NAME_REGEX + ")").matcher(hiveSql);
    if (m.lookingAt()) {
      sql.append("delete from ")
          .append(translateTableNames(m.group(1)));
      if (m.end() < hiveSql.length()) {
        sql.append(translateExpressions(hiveSql.substring(m.end())));
      }
      return sql.toString();
    } else {
      throw new TranslationException("delete", hiveSql);
    }
  }

  /**********************************************************************************************
   * Utility functions
   **********************************************************************************************/

  private class Quote {
    final int number;
    final char quoteType;
    String value;

    public Quote(int number, char quoteType, String value) {
      this.number = number;
      this.quoteType = quoteType;
      this.value = new String(value);
    }
  }

  private Map<String, Quote> quotes = new HashMap<>();
  private static int numQuotes = 1;

  private String deQuote(String hiveSql) {
    // Take all of the quoted strings out, replacing them with non-sense strings but saving them
    // in a map.  We'll put them back at the end.  This avoids the need to modify regex's for
    // them and it keeps us from getting bogus hits when we're looking for keywords.
    char prev = 0;
    char currentQuote = 0;
    StringBuilder output = new StringBuilder();
    StringBuilder quoteValue = new StringBuilder();
    for (char c : hiveSql.toCharArray()) {
      if (currentQuote == 0) {
        if (c == '`' || c == '"' || c == '\'') {
          currentQuote = c;
        } else {
          output.append(c);
        }
      } else if (c == currentQuote && prev == 0) {
        // First instance of a quote, can't close it yet as the next character may be a quote to,
        // so this is just an escape
        prev = c; // set up for next pass
        quoteValue.append(c);
      } else if (c != currentQuote && prev == currentQuote) {
        // okay, now we've seen a closing quote and the next char is not another quote, so close it.
        String marker = QUOTE_START + numQuotes + QUOTE_END;
        output.append(marker);
        quoteValue.setLength(quoteValue.length() - 1); // trim the final quote
        quotes.put(marker, new Quote(numQuotes++, currentQuote, quoteValue.toString()));
        quoteValue = new StringBuilder();
        currentQuote = 0;
        prev = 0;
        output.append(c);
      } else if (c == currentQuote && prev == currentQuote) {
        // it was an escaped quote.
        prev = 0;
        quoteValue.append(c);
      } else {
        quoteValue.append(c);
      }
    }
    // We could have ended on the quote
    if (currentQuote != 0 && (prev == '`' || prev == '"' || prev == '\'')) {
      String marker = QUOTE_START + numQuotes + QUOTE_END;
      output.append(marker);
      quoteValue.setLength(quoteValue.length() - 1); // trim the final quote
      quotes.put(marker, new Quote(numQuotes, currentQuote, quoteValue.toString()));
    }

    return output.toString();
  }

  private String reQuote(String benchSql) {
    for (Quote quote : quotes.values()) {
      Matcher m = Pattern.compile(QUOTE_START + quote.number + QUOTE_END).matcher(benchSql);
      if (quote.quoteType == '`') {
        benchSql = m.replaceAll(identifierQuote() + quote.value + identifierQuote());
      } else {
        benchSql = m.replaceAll(quote.quoteType + quote.value + quote.quoteType);
      }
    }
    quotes.clear();
    return benchSql;
  }

  protected abstract char identifierQuote();

}
