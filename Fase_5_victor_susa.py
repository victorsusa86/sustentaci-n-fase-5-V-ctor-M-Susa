# fase_5_Victor_Susa_213022_25

def calcular_jornada(recurso):
    """
    Calcula el total de horas trabajadas
    y clasifica la jornada laboral.
    """
    total = sum(recurso[1:])

    if total > 40:
        clasificacion = "Sobretiempo"
    else:
        clasificacion = "Horario estándar"

    return total, clasificacion


# Matriz
matriz = []

print("REGISTRO DE HORAS TRABAJADAS\n")

for i in range(4):
    print(f"\nRecurso {i + 1}")

    nombre = input("Nombre: ")

    lunes = float(input("Horas lunes: "))
    martes = float(input("Horas martes: "))
    miercoles = float(input("Horas miércoles: "))
    jueves = float(input("Horas jueves: "))
    viernes = float(input("Horas viernes: "))

    matriz.append([
        nombre,
        lunes,
        martes,
        miercoles,
        jueves,
        viernes
    ])

print("\n" + "=" * 60)
print("REPORTE DE HORAS SEMANALES")
print("=" * 60)

for recurso in matriz:

    total, clasificacion = calcular_jornada(recurso)

    print(f"\nNombre: {recurso[0]}")
    print(f"Total de horas: {total}")
    print(f"Clasificación: {clasificacion}")
    