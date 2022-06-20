package spark

object GetCountry {
    def getCountryTwitter(location: String): String= {
        println("Calling _getCountryInfo ...")
        var country = "NULL"
        if (location.contains(",")) {
            val locArray = location.split(",")
            country = locArray(locArray.length - 1)
            val cleanCountry = country
                .replaceAll("""[\p{Punct}&&[^a-zA-Z]]]""", "")
            if (cleanCountry.trim().length == 2 && locArray.length == 2) {
                country = "USA"
            }
        }
        country.trim().toUpperCase()
    }
}
