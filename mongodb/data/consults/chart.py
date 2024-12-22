
import matplotlib.pyplot as plt
import numpy as np

# Inicializar listas para las categorías y los valores
categories = []
values = []

# Leer el archivo CSV utilizando open()
with open('./mongodb/data/consults/tmp/meanNO2centrebyday.csv', 'r', newline='') as file:
    # Omitir el encabezado
    next(file)
    for line in file:
        nombre, dia, meanNO2 = line.strip().split(',')
        categories.append(int(dia))  # Convertir el día a entero
        values.append(float(meanNO2))

# Ordenar por el valor de día (de 1 a 31)
sorted_data = sorted(zip(categories, values))
categories_sorted, values_sorted = zip(*sorted_data)

# Crear una lista de días del 1 al 31
all_days = list(range(1, 32))

# Asegurarse de que todos los días del 1 al 31 están representados
full_values = [0] * 31  # Inicializar con ceros
for day, value in zip(categories_sorted, values_sorted):
    full_values[day - 1] = value  # Asignar el valor correspondiente al día

# NO2 min and max
min_index = full_values.index(min(full_values))
max_index = full_values.index(max(full_values))

colors = ['grey'] * len(full_values)
colors[min_index] = 'green'
colors[max_index] = 'red'

# Crear el gráfico de barras
plt.bar(all_days, full_values, color=colors)

# Ajustar la visibilidad de las etiquetas del eje X (todos los días)
plt.xlabel('Day', fontsize='15')
plt.ylabel('NO2 level', fontsize='15')
plt.title('NO2 values in Valencia in May 2024', fontsize='18')

# Ajustar los ticks
plt.xticks(all_days, fontsize='10', rotation=0)

# Ajustar los datos para la tendencia (línea)
x_values = np.array(all_days)  # Días
y_values = np.array(full_values)  # Niveles de NO2

# Ajuste polinómico de primer grado (línea recta)
slope, intercept = np.polyfit(x_values, y_values, 1)
trendline = slope * x_values + intercept

# Graficar la línea de tendencia
plt.plot(x_values, trendline, color='blue', linestyle='--', label='Trendline')

# Mostrar la leyenda
plt.legend()

# Ajustar el layout para evitar solapamientos
plt.tight_layout()

# Mostrar el gráfico
plt.show()
