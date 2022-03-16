package com.github.calmera.eda.todo;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.servlet.mvc.method.RequestMappingInfoHandlerMapping;
import springfox.documentation.spring.web.plugins.WebFluxRequestHandlerProvider;
import springfox.documentation.spring.web.plugins.WebMvcRequestHandlerProvider;
import springfox.documentation.swagger.web.InMemorySwaggerResourcesProvider;
import springfox.documentation.swagger.web.SwaggerResource;
import springfox.documentation.swagger.web.SwaggerResourcesProvider;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

@Configuration
public class SpringFoxConfig {

    @Configuration
    public static class SwaggerSpecConfig {

        // TODO temporary fix for a bug in SpringFox, see
        // https://github.com/spring-projects/spring-boot/issues/28794 and
        // https://github.com/springfox/springfox/issues/3462
        @Bean
        public static BeanPostProcessor springfoxHandlerProviderBeanPostProcessor() {
            return new BeanPostProcessor() {

                @Override
                public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
                    if (bean instanceof WebMvcRequestHandlerProvider || bean instanceof WebFluxRequestHandlerProvider) {
                        customizeSpringfoxHandlerMappings(getHandlerMappings(bean));
                    }
                    return bean;
                }

                private <T extends RequestMappingInfoHandlerMapping> void customizeSpringfoxHandlerMappings(
                        List<T> mappings) {
                    List<T> copy = mappings.stream().filter((mapping) -> mapping.getPatternParser() == null).toList();
                    mappings.clear();
                    mappings.addAll(copy);
                }

                @SuppressWarnings("unchecked")
                private List<RequestMappingInfoHandlerMapping> getHandlerMappings(Object bean) {
                    try {
                        Field field = ReflectionUtils.findField(bean.getClass(), "handlerMappings");
                        field.setAccessible(true);
                        return (List<RequestMappingInfoHandlerMapping>) field.get(bean);
                    }
                    catch (IllegalArgumentException | IllegalAccessException ex) {
                        throw new IllegalStateException(ex);
                    }
                }
            };
        }

        @Primary
        @Bean
        public SwaggerResourcesProvider swaggerResourcesProvider(
                InMemorySwaggerResourcesProvider defaultResourcesProvider) {
            return () -> {
                SwaggerResource wsResource = new SwaggerResource();
                wsResource.setName("TR Igestor API");
                wsResource.setSwaggerVersion("3.0.0");
                wsResource.setLocation("/openapi.yaml");

                List<SwaggerResource> resources = new ArrayList<>(defaultResourcesProvider.get());
                resources.clear();
                resources.add(wsResource);
                return resources;
            };
        }

    }

}

