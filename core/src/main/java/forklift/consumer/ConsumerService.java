package forklift.consumer;

import forklift.decorators.BeanResolver;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;


public class ConsumerService {
	private Class<?> clazz;
	private Object instance;
	private List<Method> beanResolvers = new ArrayList<>();

	public ConsumerService(Class<?> clazz) {
		this.clazz = clazz;

		try {
			this.instance = clazz.newInstance();

			for (Method m : clazz.getDeclaredMethods())
                if (m.isAnnotationPresent(BeanResolver.class))
                    beanResolvers.add(m);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}