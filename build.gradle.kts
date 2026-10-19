import io.spring.gradle.dependencymanagement.dsl.DependencyManagementExtension
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension
import org.springframework.boot.gradle.plugin.SpringBootPlugin

plugins {
    kotlin("jvm") version "2.3.21" apply false
    kotlin("plugin.spring") version "2.3.21" apply false
    kotlin("plugin.jpa") version "2.3.21" apply false
    id("org.springframework.boot") version "4.1.1" apply false
    id("io.spring.dependency-management") version "1.1.7" apply false
}

allprojects {
    group = "io.github.hyeokjinon"
    version = "0.1.0-SNAPSHOT"
    repositories { mavenCentral() }
}

// ":modules", ":apps" 는 폴더용 컨테이너라 제외하고, 실제 모듈에만 공통 설정을 건다
val codeProjects = subprojects.filter { it.path.count { c -> c == ':' } == 2 }

configure(codeProjects) {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "io.spring.dependency-management")

    // Spring Boot BOM 으로 Netty·SLF4J·JUnit 버전을 한곳에서 맞춘다
    configure<DependencyManagementExtension> {
        imports { mavenBom(SpringBootPlugin.BOM_COORDINATES) }
    }

    configure<KotlinJvmProjectExtension> {
        jvmToolchain(21)
        compilerOptions {
            freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
        }
    }

    dependencies {
        "implementation"("org.slf4j:slf4j-api")
        "testImplementation"("org.jetbrains.kotlin:kotlin-test-junit5")
        "testImplementation"("org.junit.jupiter:junit-jupiter")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
        "testRuntimeOnly"("ch.qos.logback:logback-classic")
    }

    tasks.withType<Test> {
        useJUnitPlatform()
        testLogging { events("passed", "skipped", "failed") }
    }
}
