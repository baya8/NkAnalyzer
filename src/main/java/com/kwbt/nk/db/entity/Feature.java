package com.kwbt.nk.db.entity;

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class Feature implements Serializable {

	private Integer race_id;

	private Integer order_of_finish;

	private Double age;

	private Double dhweight;

	private Double distance;

	private Double dsl;

	private Double hweight;

	private String sex;

	private String surface;

	private String weather;

	private String course;

	private Double odds;

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

	public Double getAge() {
		return age;
	}

	public void setAge(Double age) {
		this.age = age;
	}

	public Double getDhweight() {
		return dhweight;
	}

	public void setDhweight(Double dhweight) {
		this.dhweight = dhweight;
	}

	public Double getDistance() {
		return distance;
	}

	public void setDistance(Double distance) {
		this.distance = distance;
	}

	public Double getDsl() {
		return dsl;
	}

	public void setDsl(Double dsl) {
		this.dsl = dsl;
	}

	public Double getHweight() {
		return hweight;
	}

	public void setHweight(Double hweight) {
		this.hweight = hweight;
	}

	public String getSex() {
		return sex;
	}

	public void setSex(String sex) {
		this.sex = sex;
	}

	public String getSurface() {
		return surface;
	}

	public void setSurface(String surface) {
		this.surface = surface;
	}

	public String getWeather() {
		return weather;
	}

	public void setWeather(String weather) {
		this.weather = weather;
	}

	public String getCourse() {
		return course;
	}

	public void setCourse(String course) {
		this.course = course;
	}

	public Double getOdds() {
		return odds;
	}

	public void setOdds(Double odds) {
		this.odds = odds;
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
