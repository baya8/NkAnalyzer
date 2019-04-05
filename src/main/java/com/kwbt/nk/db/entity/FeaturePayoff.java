package com.kwbt.nk.db.entity;

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class FeaturePayoff implements Serializable {

	private Integer race_id;

	private Integer order_of_finish;

	public Integer getRace_id() {
		return race_id;
	}

	public void setRace_id(Integer race_id) {
		this.race_id = race_id;
	}

	public Integer getOrder_of_finish() {
		return order_of_finish;
	}

	public void setOrder_of_finish(Integer order_of_finish) {
		this.order_of_finish = order_of_finish;
	}


	@Override
	public int hashCode() {
		return HashCodeBuilder.reflectionHashCode(this);
	}

	@Override
	public boolean equals(Object obj) {
		return EqualsBuilder.reflectionEquals(this, obj);
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
	}

}
