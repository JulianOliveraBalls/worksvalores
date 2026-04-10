import random
import logging
OPERADORES = [
    "mlucero", "scordoba", "yalbornoz", "abizzotto", 
    "mpanella", "rescribano", "vcerda", "opmendoza2", 
    "opmendoza3", "avillegas", "ycollado", "pzarate", 
    "mcairat", "acarrai"
]

ESTADO_EVENTO_MAP = {
    "No Cobró Sueldo": "AVITITUL",
    "Sin Empleo": "AVITITUL",
    "No Puede Pagar": "AVITITUL",
    "Vacaciones": "AVITITUL",
    "Desconoce Deuda": "AVITITUL",
    "Llamada Recurrente": "AVITITUL",
    "Falleció": "AVITITUL",
    "Problema Económico": "AVITITUL",
    "Titular Cortó": "AVITITUL",
    "Enfermedad": "AVITITUL",
    "Ocupado": "NOCONTESTA",
    "Llamando": "NOCONTESTA",
    "Indefinido": "NOCONTESTA",
    "Correo de Voz": "CAUT",
    "Cortó Llamada": "NOCONTESTA",
    "Número Equivocado": "NOCORRESP",
    "No Titular": "NOCORRESP",
    "Transferir": "AVITITUL",
    "Volver a Llamar": "NOCONTESTA",
    "Pago Total": "AVITITUL",
    "Refinanciar": "AVITITUL",
    "Compromete Fecha": "PRPG",
    "Compromete Sin Fecha": "PRPG",
    "Compromete Fecha - Pago Parcial": "PRPG",
    "Compromete Sin Fecha - Pago Parcial": "PRPG",
    "Posterga Pago": "AVITITUL",
    "Posterga Pago - Pago Parcial": "PRPG",
    "Insulto": "NOCORRESP",
    "Pago Incompleto": "AVITITUL",
}

def map_evento(estado):
    if estado not in ESTADO_EVENTO_MAP:
        logging.warning(f"Estado no mapeado: {estado}")
    return ESTADO_EVENTO_MAP.get(estado, "NOCONTESTA")


def get_estados():
    return list(ESTADO_EVENTO_MAP)

COMENTARIOS = {
    "No Cobró Sueldo": [
        "Se contacta a titular al telefono {telefono}. Se le informa de su deuda y forma de pagar. No puede aceptar condiciones. Motivo: Aun no percibe sus haberes.",
        "Se establece comunicacion al {telefono}. El cliente manifiesta voluntad de pago pero indica que no se le ha acreditado el sueldo. Motivo: Pendiente de cobro.",
        "Contacto efectivo con el titular ({telefono}). Informa que por retrasos administrativos de su empleador aun no dispone de su salario. Motivo: Sueldo no cobrado."
    ],
    "Sin Empleo": [
        "Se contacta a titular en el telefono {telefono}. Se le informa de su deuda y forma de pagar. No puede aceptar condiciones. Motivo: Actualmente desempleado.",
        "Llamada al {telefono}. El cliente explica que se encuentra sin actividad laboral y no posee ingresos fijos. Motivo: Falta de empleo.",
        "Se mantiene dialogo con el titular al {telefono}. Refiere que perdio su puesto de trabajo recientemente y no puede afrontar compromisos. Motivo: Cesante."
    ],
    "No Puede Pagar": [
        "Se contacta a titular en el telefono {telefono}. Se le informa de su deuda y forma de pagar. No puede aceptar condiciones. Motivo: Insolvencia economica.",
        "Comunicacion al {telefono}. El cliente reconoce la deuda pero manifiesta una imposibilidad total de pago en este momento. Motivo: Sin recursos disponibles.",
        "Se gestiona cobro al {telefono}. El titular indica que sus gastos actuales superan sus ingresos y no puede cumplir. Motivo: Falta de capacidad de pago."
    ],
    "Vacaciones": [
        "Se logra contacto al {telefono}. El titular indica que se encuentra fuera de la ciudad por receso vacacional. Motivo: Ausencia por vacaciones.",
        "Llamada atendida al {telefono}. El cliente solicita ser contactado a su regreso ya que se encuentra de viaje. Motivo: Vacaciones.",
        "Se contacta al titular ({telefono}). Refiere estar en periodo de descanso y no tiene acceso a sus medios de pago. Motivo: Cliente de vacaciones."
    ],
    "Desconoce Deuda": [
        "Se contacta a titular en el telefono {telefono}. Se le informa de su deuda. El mismo manifiesta no reconocer el producto o el saldo. Motivo: Desconoce deuda.",
        "Comunicacion al {telefono}. El cliente indica que no ha realizado consumos con la entidad y desconoce el origen del reclamo. Motivo: Error en registro o desconocimiento.",
        "El titular atiende al {telefono} y reclama que no posee deudas pendientes, solicitando revision de cuenta. Motivo: Desconocimiento de obligacion."
    ],
    "Llamada Recurrente": [
        "Se contacta al {telefono}. El titular se muestra molesto indicando que ya fue contactado previamente en el dia. Motivo: Reclama reiteracion de llamadas.",
        "Llamada al {telefono}. El cliente interrumpe la gestion alegando que ya brindo una respuesta en un contacto anterior. Motivo: Llamada recurrente.",
        "Se establece contacto al {telefono}. El titular refiere que la insistencia telefonica es excesiva y corta la comunicacion. Motivo: Gestion repetida."
    ],
    "Falleció": [
        "Se llama al {telefono}. Atiende un familiar e informa el deceso del titular. Se solicitan datos para el area legal. Motivo: Fallecimiento.",
        "Contacto al {telefono}. Se toma conocimiento de que el cliente ha fallecido a traves de un tercero en el domicilio. Motivo: Titular fallecido.",
        "Comunicacion con el entorno del titular al {telefono}. Informan el fallecimiento del mismo. Motivo: Defuncion."
    ],
    "Problema Económico": [
        "Se contacta a titular en el telefono {telefono}. Se le informa de su deuda. Manifiesta voluntad pero atraviesa una crisis financiera. Motivo: Problemas economicos.",
        "Llamada al {telefono}. El cliente explica que tiene otras deudas prioritarias y su flujo de caja es negativo. Motivo: Inestabilidad financiera.",
        "Se asesora al titular al {telefono}. Indica que debido a gastos imprevistos no puede cubrir la cuota este mes. Motivo: Dificultad economica."
    ],
    "Titular Cortó": [
        "Se establece contacto al {telefono}. Al momento de informar el motivo de la llamada, el titular finaliza la comunicacion. Motivo: Corte abrupto.",
        "Llamada al {telefono}. El cliente atiende pero procede a colgar sin mediar palabra tras nuestra identificacion. Motivo: Titular corto llamada.",
        "Se intenta gestionar al {telefono}, pero el titular interrumpe la conexion de forma intencional. Motivo: Desconexion por parte del cliente."
    ],
    "Enfermedad": [
        "Se contacta al titular al {telefono}. Informa que por motivos de salud personales no puede realizar gestiones de pago. Motivo: Problema medico.",
        "Comunicacion al {telefono}. El cliente refiere estar atravesando una situacion de enfermedad familiar que consume sus recursos. Motivo: Gastos medicos / Salud.",
        "Llamada atendida al {telefono}. El titular manifiesta imposibilidad de acercarse a pagar por reposo medico. Motivo: Enfermedad."
    ],
    "Compromete Fecha": [
        "Se contacta a titular al {telefono}. Se acuerda la regularizacion total del saldo para la fecha estipulada. Motivo: Promesa de pago confirmada.",
        "El cliente atiende al {telefono} y confirma que el pago sera acreditado el dia pactado en la gestion. Motivo: Compromiso con fecha.",
        "Se establece acuerdo de pago con el titular al {telefono}. El mismo asegura contar con los fondos para la fecha acordada. Motivo: Fecha de pago pactada."
    ],
    "Compromete Sin Fecha": [
        "Se contacta al titular al {telefono}. Manifiesta que pagara a la brevedad, aunque no puede precisar un dia exacto. Motivo: Compromiso de pago proximo.",
        "Comunicacion al {telefono}. El cliente reconoce la deuda y afirma que enviara el pago durante la semana. Motivo: Promesa sin fecha fija.",
        "El titular ({telefono}) asegura que regularizara su situacion en cuanto disponga del efectivo. Motivo: Voluntad de pago sin fecha confirmada."
    ],
    "Compromete Fecha - Pago Parcial": [
        "Se contacta al {telefono}. El titular se compromete a realizar un pago a cuenta en la fecha acordada para reducir la mora. Motivo: Pago parcial con fecha.",
        "Llamada al {telefono}. El cliente acuerda abonar una parte del total en el plazo estipulado. Motivo: Compromiso de abono parcial.",
        "Se gestiona pago parcial al {telefono}. El titular confirma el monto y la fecha en la que realizara el deposito. Motivo: Acuerdo parcial programado."
    ],
    "Compromete Sin Fecha - Pago Parcial": [
        "Se contacta al titular al {telefono}. Refiere que hara una entrega parcial de fondos en los proximos dias. Motivo: Promesa de pago parcial.",
        "Comunicacion al {telefono}. El cliente indica voluntad de ir reduciendo el saldo mediante pagos parciales segun disponibilidad. Motivo: Abono parcial sin fecha.",
        "El titular ({telefono}) se compromete a realizar un pago a cuenta a la brevedad posible. Motivo: Compromiso parcial pendiente."
    ],
    "Ocupado": [
        "Se intenta contacto al {telefono}, pero la linea se encuentra ocupada actualmente. Se reintentara luego. Motivo: Linea ocupada.",
        "Comunicacion fallida al {telefono}. El interno da señal de ocupado. Motivo: Ocupado.",
        "Intento de gestion al {telefono} sin exito por linea ocupada. Se deja constancia para proximo barrido. Motivo: Ocupado."
    ],
    "Correo de Voz": [
        "Se establece contacto con el {telefono}, el cual deriva directamente a la casilla de mensajes. No se deja recado. Motivo: Correo de voz.",
        "Llamada al {telefono} no atendida por el titular, salta el contestador automatico. Motivo: Buzon de voz.",
        "Intento de comunicacion al {telefono}. La llamada es desviada al correo de voz tras varios tonos. Motivo: Contesta buzon."
    ],
    "Posterga Pago": [
        "Se contacta al titular al {telefono}. Solicita mas tiempo para realizar el pago debido a compromisos previos. Motivo: Postergacion de pago.",
        "Llamada al {telefono}. El cliente indica que no puede abonar hoy y pide ser contactado nuevamente en unos dias. Motivo: Pide postergar.",
        "El titular ({telefono}) manifiesta voluntad de pago pero solicita desplazar la fecha de gestion por motivos personales. Motivo: Posterga gestion."
    ],
    "Posterga Pago - Pago Parcial": [
        "Comunicacion al {telefono}. El cliente solicita postergar el pago parcial acordado por falta de liquidez inmediata. Motivo: Posterga abono parcial.",
        "Se gestiona al titular al {telefono}. Refiere que realizara una entrega minima mas adelante en el mes. Motivo: Pago parcial postergado.",
        "El titular ({telefono}) indica que no llegara a cubrir el monto parcial hoy y pide unos dias de prorroga. Motivo: Prorroga de pago parcial."
    ],
    "Insulto": [
        "Se establece contacto al {telefono}, pero el interlocutor se expresa de forma agresiva e insulta al operador. Motivo: Maltrato / Agresividad.",
        "Comunicacion fallida al {telefono}. El receptor utiliza lenguaje inapropiado impidiendo la gestion de cobro. Motivo: Trato hostil.",
        "Se intenta gestionar deuda al {telefono}. El titular reacciona con insultos y agresiones verbales. Motivo: Cliente agresivo."
    ],
    "Pago Incompleto": [
        "Se contacta al {telefono}. El titular informa que solo pudo realizar un pago menor al compromiso asumido. Motivo: Pago realizado incompleto.",
        "El cliente indica al {telefono} que el deposito efectuado no cubre el total de la mora actual. Motivo: Saldo remanente tras pago.",
        "Verificacion con el titular al {telefono}. Manifiesta haber pagado una parte pero reconoce saldo pendiente. Motivo: Pago parcial ejecutado."
    ],
    "Cortó Llamada": [
        "Se inicia el contacto al {telefono}, pero el interlocutor finaliza la llamada de forma abrupta. Motivo: Corto comunicacion.",
        "Comunicacion interrumpida al {telefono} tras presentarnos. No se logra retomar el dialogo. Motivo: Colgaron llamada.",
        "Se intenta gestionar deuda al {telefono}, pero el receptor corta la comunicacion inmediatamente. Motivo: Llamada cortada."
    ],
    "Número Equivocado": [
        "Se contacta al {telefono}. Atiende una persona que indica no conocer al titular. Motivo: Telefono erroneo.",
        "Llamada al {telefono}. El interlocutor manifiesta que el numero no pertenece a la persona buscada. Motivo: Numero equivocado.",
        "Se establece contacto al {telefono}, pero informan que el titular ya no posee esta linea. Motivo: Datos desactualizados."
    ],
    "Indefinido": [
        "Se intenta gestionar el caso al {telefono}, pero la respuesta obtenida no es clara o la comunicacion es deficiente. Motivo: Sin definicion.",
        "Contacto al {telefono} con resultado ambiguo. No se logra categorizar la respuesta del cliente. Motivo: Indefinido.",
        "Llamada al {telefono}. Se percibe ruido de linea o respuesta nula por parte del receptor. Motivo: Resultado indefinido."
    ],
    "No Titular": [
        "Se contacta al {telefono}. Atiende un tercero quien informa que el titular no se encuentra disponible. Motivo: Contacto con tercero.",
        "Comunicacion al {telefono}. Se habla con un allegado del cliente, quien no esta autorizado a tomar compromisos. Motivo: No es el titular.",
        "Llamada atendida por otra persona al {telefono}. Se solicita al titular pero informan que no puede atender en este momento. Motivo: Tercero atiende."
    ],
    "Transferir": [
        "Se contacta al {telefono}. El cliente solicita ser derivado con un supervisor o area especifica para tratar su caso. Motivo: Solicitud de transferencia.",
        "Comunicacion al {telefono}. Tras explicar la deuda, el titular pide hablar con otro departamento administrativo. Motivo: Derivacion solicitada.",
        "Gestion al {telefono}. Se procede a marcar para transferencia interna segun requerimiento del cliente. Motivo: Transferir llamada."
    ],
    "Volver a Llamar": [
        "Se contacta al titular al {telefono}. Manifiesta estar ocupado y solicita que se le contacte en otro horario. Motivo: Volver a llamar.",
        "Llamada atendida al {telefono}. El cliente pide retomar la comunicacion mas tarde para analizar su situacion. Motivo: Rellamar.",
        "El titular indica al {telefono} que no puede hablar en este momento. Se agenda nuevo contacto. Motivo: Solicita nueva llamada."
    ],
    "Pago Total": [
        "Se establece contacto al {telefono}. El titular informa que ya ha realizado el pago total de su deuda. Se procede a verificar en sistema. Motivo: Ya pago.",
        "Comunicacion con el cliente al {telefono}. Manifiesta haber cancelado la totalidad del saldo pendiente anteriormente. Motivo: Pago ya efectuado.",
        "El titular atiende al {telefono} e indica que su cuenta ya se encuentra al dia tras haber realizado el pago integro. Motivo: Cliente informa pago total."
    ],
    "Refinanciar": [
        "Se contacta al titular al {telefono}. Solicita un plan de cuotas o refinanciacion para poder afrontar la deuda. Motivo: Pedido de refinanciacion.",
        "Llamada al {telefono}. El cliente manifiesta voluntad de pago pero requiere nuevas condiciones de financiacion. Motivo: Solicita plan de pagos.",
        "El titular indica al {telefono} que solo puede cumplir mediante una reestructuracion de su saldo actual. Motivo: Refinanciacion."
    ],
    "Llamando": [
        "Se inicia proceso de marcacion al {telefono}. Gestion en curso. Motivo: Llamando.",
        "Intento de salida de llamada al {telefono}. Esperando respuesta del suscriptor. Motivo: En llamada.",
        "Estableciendo conexion con el nro {telefono} para gestion de cobro. Motivo: Llamando."
    ]
}

def get_operador():
    return random.choice(OPERADORES)


def get_comentario(estado, index, telefono):
    tel_str = str(telefono) if telefono and str(telefono) != 'nan' else "no registrado"
    
    lista = COMENTARIOS.get(estado, ["Gestión realizada al teléfono {telefono}"])
    
    # Seleccionamos la frase según el índice
    frase = lista[index % len(lista)]
    
    return frase.format(telefono=tel_str)