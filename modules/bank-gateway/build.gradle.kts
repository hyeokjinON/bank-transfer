plugins { `java-library` }

dependencies {
    api(project(":modules:bank-protocol"))
    testImplementation(project(":apps:mock-bank"))
}
