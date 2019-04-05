package com.kwbt.nk.common;

import java.io.Serializable;
import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class FeatureMatcher implements Serializable {

	public int count = 0;

	public int age = 0;
	public int course = 0;
	public int dhweight = 0;
	public int distance = 0;
	public int dsl = 0;
	public int hweight = 0;
	public int odds = 0;
	public int sex = 0;
	public int surface = 0;
	public int weather = 0;

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}

	@Override
	public boolean equals(Object obj) {

		if (!(obj instanceof FeatureMatcher)) {
			return false;
		}

		FeatureMatcher e = (FeatureMatcher) obj;

		return Objects.equals(age, e.age)
				&& Objects.equals(course, e.course)
				&& Objects.equals(dhweight, e.dhweight)
				&& Objects.equals(distance, e.distance)
				&& Objects.equals(dsl, e.dsl)
				&& Objects.equals(hweight, e.hweight)
				&& Objects.equals(odds, e.odds)
				&& Objects.equals(sex, e.sex)
				&& Objects.equals(surface, e.surface)
				&& Objects.equals(weather, e.weather);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + age;
		result = prime * result + course;
		result = prime * result + dhweight;
		result = prime * result + distance;
		result = prime * result + dsl;
		result = prime * result + hweight;
		result = prime * result + odds;
		result = prime * result + sex;
		result = prime * result + surface;
		result = prime * result + weather;
		return result;
	}

}
