package forklift.consumer;

import forklift.decorators.BeanResolver;
import forklift.decorators.OnDeploy;
import forklift.decorators.OnUndeploy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;


public class ConsumerService {
	private static final Logger log = LoggerFactory.getLogger(ConsumerService.class);

	private Class<?> clazz;
	private Object instance;
	private List<Method> beanResolvers = new ArrayList<>();
	private List<Method> onDeploy = new ArrayList<>();
	private List<Method> onUndeploy = new ArrayList<>();

	public ConsumerService(Class<?> clazz) 
	  throws Exception {
		this(clazz, clazz.newInstance());
	}

	public ConsumerService(Object instance) {
		this(instance.getClass(), instance);
	}

	public ConsumerService(Class<?> clazz, Object instance) {
		this.clazz = clazz;
		this.instance = instance;

		try {
			for (Method m : clazz.getDeclaredMethods()) {
                if (m.isAnnotationPresent(BeanResolver.class))
                    beanResolvers.add(m);
                else if (m.isAnnotationPresent(OnDeploy.class))
                	onDeploy.add(m);
                else if (m.isAnnotationPresent(OnUndeploy.class))
                	onUndeploy.add(m);
            }
		} catch (Exception e) {
			log.error("Unable to init consumer service", e);
		}
	}

	/**
	 * Resolve a class to an object using any available bean resolvers.
	 * @param  c    class type
	 * @param  name name of the field.
	 * @return      [description]
	 */
	public Object resolve(Class c, String name)
	  throws Exception {
		// Attempt to resolve the class via name and type.
		for (Method m : beanResolvers) {
			final Object o = m.invoke(instance, c, name);

			if (o != null)
				return o;
		}

		return null;
	}

	/**
	 * Invoke all onDeploy annotated methods.
	 */
	public void onDeploy()
	  throws Exception {
		for (Method m : onDeploy)
			m.invoke(instance);
	}

	/**
	 * Invoke all onUndeploy annotated methods.
	 */
	public void onUndeploy()
	  throws Exception {
		for (Method m : onUndeploy)
			m.invoke(instance);
	}
}