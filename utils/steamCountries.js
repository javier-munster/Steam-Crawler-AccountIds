'use strict';
const steamCountries = require('./steam_countries.min.json');

function getLocation(country, state, city) {
    const ret = {};

    try {
        ret.country = steamCountries[country].name.toString();
    } catch (err) {
        console.log("Could not get country:", country);

        if (!!country) {
            ret.country = country.toString();
        }
    }

    try {
        ret.state = steamCountries[country].states[state].name.toString();
    } catch (err) {
        console.log("Could not get state:", state);

        if (!!state) {
            ret.state = state.toString();
        }
    }

    try {
        ret.city = steamCountries[country].states[state].cities[city].name.toString();
    } catch (err) {
        console.log("Could not get city:", city);

        if (!!city) {
            ret.city = city.toString();
        }
    }

    return ret;
}

module.exports = {
    getLocation
};