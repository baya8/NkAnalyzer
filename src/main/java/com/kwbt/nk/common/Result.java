package com.kwbt.nk.common;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class Result implements Serializable {

	public int count = 0;

	public int age = -1;
	public int course = -1;
	public int dhweight = -1;
	public int distance = -1;
	public int dsl = -1;
	public int hweight = -1;
	public int odds = -1;
	public int sex = -1;
	public int surface = -1;
	public int weather = -1;

	public int bakenAllNum = -1;
	public int bakenNumOfFinishedOne = -1;
	public int payoffSum = -1;
	public double kaisyu = -1.;
	public double syoritsu = -1.;

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}

	public Map<String, Object> getValueMap() {

		return new TreeMap<String, Object>() {
			{
				put(CommonConst.STR_AGE, age);
				put(CommonConst.STR_COURSE, course);
				put(CommonConst.STR_DHWEIGHT, dhweight);
				put(CommonConst.STR_DISTANCE, distance);
				put(CommonConst.STR_DSL, dsl);
				put(CommonConst.STR_HWEIGHT, hweight);
				put(CommonConst.STR_ODDS, odds);
				put(CommonConst.STR_SEX, sex);
				put(CommonConst.STR_SURFACE, surface);
				put(CommonConst.STR_WEATHER, weather);

				put(CommonConst.STR_COUNT, count);
				put(CommonConst.STR_BAKENALLNUM, bakenAllNum);
				put(CommonConst.STR_MONEYBAKEN, bakenNumOfFinishedOne);
				put(CommonConst.STR_PAYOFFSUM, payoffSum);
				put(CommonConst.STR_KAISYU, kaisyu);
				put(CommonConst.STR_WINPER, syoritsu);
			}
		};
	}
}
