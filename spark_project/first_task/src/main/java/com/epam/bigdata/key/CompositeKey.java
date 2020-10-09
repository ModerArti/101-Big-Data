package com.epam.bigdata.key;

import java.io.Serializable;
import java.util.Objects;

public class CompositeKey implements Serializable {

    private String id;
    private String continent;
    private String country;
    private String market;

    public CompositeKey(String id, String continent, String country, String market) {
        this.id = id;
        this.continent = continent;
        this.country = country;
        this.market = market;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompositeKey that = (CompositeKey) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(continent, that.continent) &&
                Objects.equals(country, that.country) &&
                Objects.equals(market, that.market);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, continent, country, market);
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
}
