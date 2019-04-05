package com.kwbt.nk.analyzer.util;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.kwbt.nk.common.FeatureMatcher;
import com.kwbt.nk.common.MatcherConst;
import com.kwbt.nk.common.Range;

public class SQLBuilder {

	private final static String BETWEEN = " between ";
	private final static String AND = " and ";
	// private final static String KAN = ",";

	// private final static String NAME_RACE_ID = "tar1.race_id";
	// private final static String NAME_ORDER_OF_FINISH = "tar1.order_of_finish";

	private final static String NAME_AGE = "tar1.age";
	private final static String NAME_COURSE = "tar1.course";
	private final static String NAME_DHWEIGHT = "tar1.dhweight";
	private final static String NAME_DISTANCE = "tar1.distance";
	private final static String NAME_DSL = "tar1.dsl";
	private final static String NAME_HWEIGHT = "tar1.hweight";
	private final static String NAME_ODDS = "tar1.odds";
	private final static String NAME_SEX = "tar1.sex";
	private final static String NAME_SURFACE = "tar1.surface";
	private final static String NAME_WEATHER = "tar1.weather";

	/**
	 * 該当条件に一致するすべての馬を取得する
	 *
	 * @param e
	 * @return
	 */
	public String createSQLAllNum(FeatureMatcher e) {

		String sql = "select * from feature tar1 where 1=1";

		if (e.age != MatcherConst.VALUE_NONE) {
			double min = MatcherConst.ageMap.get(e.age).getMin();
			double max = MatcherConst.ageMap.get(e.age).getMax();

			sql += buildBetween(NAME_AGE, min, max);
		}

		if (e.course != MatcherConst.VALUE_NONE) {
			List<String> eles = Arrays.asList(MatcherConst.courseMap.get(e.course));
			sql += buildIn(NAME_COURSE, eles);
		}

		if (e.dhweight != MatcherConst.VALUE_NONE) {
			double min = MatcherConst.dhweightMap.get(e.dhweight).getMin();
			double max = MatcherConst.dhweightMap.get(e.dhweight).getMax();

			sql += buildBetween(NAME_DHWEIGHT, min, max);
		}

		if (e.distance != MatcherConst.VALUE_NONE) {
			double min = MatcherConst.distanceMap.get(e.distance).getMin();
			double max = MatcherConst.distanceMap.get(e.distance).getMax();

			sql += buildBetween(NAME_DISTANCE, min, max);
		}

		if (e.dsl != MatcherConst.VALUE_NONE) {

			Range<Double> range = MatcherConst.dslMap.get(e.dsl);

			if (range == null) {
				sql += AND + NAME_DSL + " is null";

			} else {
				double min = MatcherConst.dslMap.get(e.dsl).getMin();
				double max = MatcherConst.dslMap.get(e.dsl).getMax();

				sql += buildBetween(NAME_DSL, min, max);
			}
		}

		if (e.hweight != MatcherConst.VALUE_NONE) {
			double min = MatcherConst.hweightMap.get(e.hweight).getMin();
			double max = MatcherConst.hweightMap.get(e.hweight).getMax();

			sql += buildBetween(NAME_HWEIGHT, min, max);
		}

		if (e.odds != MatcherConst.VALUE_NONE) {
			double min = MatcherConst.oddsMap.get(e.odds).getMin();
			double max = MatcherConst.oddsMap.get(e.odds).getMax();

			sql += buildBetween(NAME_ODDS, min, max);
		}

		if (e.sex != MatcherConst.VALUE_NONE) {
			List<String> eles = Arrays.asList(MatcherConst.sexMap.get(e.sex));
			sql += buildIn(NAME_SEX, eles);
		}

		if (e.surface != MatcherConst.VALUE_NONE) {
			List<String> eles = Arrays.asList(MatcherConst.surfaceMap.get(e.surface));
			sql += buildIn(NAME_SURFACE, eles);
		}

		if (e.weather != MatcherConst.VALUE_NONE) {
			List<String> eles = Arrays.asList(MatcherConst.weatherMap.get(e.weather));
			sql += buildIn(NAME_WEATHER, eles);
		}

		return sql;
	}

	private String buildBetween(String str1, double min, double max) {

		return AND + str1 + BETWEEN + min + AND + max;
	}

	private String buildIn(String str1, List<String> eles) {

		String line = "";
		for (Iterator<String> ite = eles.iterator(); true;) {
			line += "'" + ite.next() + "'";
			if (ite.hasNext()) {
				line += ",";
			} else {
				break;
			}
		}
		return AND + str1 + " in (" + line + ")";
	}

	public String createSQLSelectPayoffOrderOfFinishIsZero(Set<Integer> c) {

		String sql = "select sum(tar1.payoff) from payoff tar1 where tar1.race_id in (";

		for (Iterator<Integer> ite = c.iterator(); ite.hasNext();) {

			sql += ite.next();

			if (ite.hasNext()) {
				sql += ",";
			}
		}

		sql += ") and tar1.ticket_type = 0";

		return sql;
	}
}
