package com.epam.bigdata.model;

import java.io.Serializable;
import java.util.Objects;

public class Hotel implements Serializable {

    private String id;
    private String continent;
    private String country;
    private String market;

    public Hotel(String id, String continent, String country, String market) {
        this.id = id;
        this.continent = continent;
        this.country = country;
        this.market = market;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Hotel that = (Hotel) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("\nHotel id: ").append(id)
                .append("\nContinent: ").append(continent)
                .append("\nCountry: ").append(country)
                .append("\nMarket: ").append(market)
                .toString();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getContinent() {
        return continent;
    }

    public void setContinent(String continent) {
        this.continent = continent;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getMarket() {
        return market;
    }

    public void setMarket(String market) {
        this.market = market;
    }
}
