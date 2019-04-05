package com.kwbt.nk.common;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class Range<T extends Comparable<T>> {

	/**
	 * Compearable minimal value
	 */
	private T min;

	/**
	 * Compearable max value
	 */
	private T max;

	/**
	 * Default Constract
	 */
	public Range() {

	}

	/**
	 * @param min
	 *            Compearable minimal value
	 * @param max
	 *            Compearable max value
	 */
	public Range(T min, T max) {
		this.min = min;
		this.max = max;
	}

	/**
	 * @return
	 */
	public T getMin() {
		return min;
	}

	/**
	 * @param min
	 */
	public void setMin(T min) {
		this.min = min;
	}

	/**
	 * @return
	 */
	public T getMax() {
		return max;
	}

	/**
	 * @param max
	 */
	public void setMax(T max) {
		this.max = max;
	}

	/**
	 * @param arg
	 * @return
	 */
	public boolean isContain(T arg) {
		return isBiggerEqual(min, arg) && isBiggerEqual(arg, max);
	}

	/**
	 * @param arg1
	 * @param arg2
	 * @return
	 */
	private boolean isBiggerEqual(T arg1, T arg2) {
		return arg1.compareTo(arg2) <= 0;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}

	@Override
	public int hashCode() {
		return HashCodeBuilder.reflectionHashCode(this);
	}

	@Override
	public boolean equals(Object obj) {
		return EqualsBuilder.reflectionEquals(this, obj);
	}

}
