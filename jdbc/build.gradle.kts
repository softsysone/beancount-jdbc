import org.gradle.api.plugins.antlr.AntlrTask

plugins {
    `java-library`
    antlr
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

dependencies {
    antlr("org.antlr:antlr4:4.13.2")
    implementation("org.antlr:antlr4-runtime:4.13.2")
    implementation("org.apache.calcite:calcite-core:1.38.0")
    implementation("org.apache.calcite:calcite-linq4j:1.38.0")
    implementation("org.apache.calcite:calcite-babel:1.38.0")
    implementation("org.apache.calcite:calcite-server:1.38.0")
    implementation("org.apache.calcite.avatica:avatica-core:1.25.0")
    implementation("org.xerial:sqlite-jdbc:3.46.1.3")

    testImplementation(platform("org.junit:junit-bom:5.10.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

val copyRuntimeLibs by tasks.registering(Sync::class) {
    from(configurations.runtimeClasspath)
    into(layout.buildDirectory.dir("runtime-libs"))
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

tasks.jar {
    dependsOn(copyRuntimeLibs)
}

tasks.withType<AntlrTask>().configureEach {
    arguments.addAll(listOf("-visitor", "-long-messages"))
}

base {
    archivesName.set("beancount-jdbc")
}

repositories {
    mavenCentral()
}

tasks.test {
    useJUnitPlatform()
}
