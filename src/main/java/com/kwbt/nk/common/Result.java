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

    public Result(FeatureMatcher f) {
        this.count = f.count;
        this.age = f.age;
        this.course = f.course;
        this.dhweight = f.dhweight;
        this.distance = f.distance;
        this.dsl = f.dsl;
        this.hweight = f.hweight;
        this.odds = f.odds;
        this.sex = f.sex;
        this.surface = f.surface;
        this.weather = f.weather;

    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getCourse() {
        return course;
    }

    public void setCourse(int course) {
        this.course = course;
    }

    public int getDhweight() {
        return dhweight;
    }

    public void setDhweight(int dhweight) {
        this.dhweight = dhweight;
    }

    public int getDistance() {
        return distance;
    }

    public void setDistance(int distance) {
        this.distance = distance;
    }

    public int getDsl() {
        return dsl;
    }

    public void setDsl(int dsl) {
        this.dsl = dsl;
    }

    public int getHweight() {
        return hweight;
    }

    public void setHweight(int hweight) {
        this.hweight = hweight;
    }

    public int getOdds() {
        return odds;
    }

    public void setOdds(int odds) {
        this.odds = odds;
    }

    public int getSex() {
        return sex;
    }

    public void setSex(int sex) {
        this.sex = sex;
    }

    public int getSurface() {
        return surface;
    }

    public void setSurface(int surface) {
        this.surface = surface;
    }

    public int getWeather() {
        return weather;
    }

    public void setWeather(int weather) {
        this.weather = weather;
    }

    public int getBakenAllNum() {
        return bakenAllNum;
    }

    public void setBakenAllNum(int bakenAllNum) {
        this.bakenAllNum = bakenAllNum;
    }

    public int getBakenNumOfFinishedOne() {
        return bakenNumOfFinishedOne;
    }

    public void setBakenNumOfFinishedOne(int bakenNumOfFinishedOne) {
        this.bakenNumOfFinishedOne = bakenNumOfFinishedOne;
    }

    public int getPayoffSum() {
        return payoffSum;
    }

    public void setPayoffSum(int payoffSum) {
        this.payoffSum = payoffSum;
    }

    public double getKaisyu() {
        return kaisyu;
    }

    public void setKaisyu(double kaisyu) {
        this.kaisyu = kaisyu;
    }

    public double getSyoritsu() {
        return syoritsu;
    }

    public void setSyoritsu(double syoritsu) {
        this.syoritsu = syoritsu;
    }

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
