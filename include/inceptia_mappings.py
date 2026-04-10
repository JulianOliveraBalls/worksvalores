import random
import logging
OPERADORES = [
    "mlucero", "scordoba", "yalbornoz", "abizzotto", 
    "mpanella", "rescribano", "vcerda", "opmendoza2", 
    "opmendoza3", "avillegas", "ycollado", "pzarate", 
    "mcairat", "acarrai"
]

ESTADO_EVENTO_MAP = {
    # AVITITUL
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

    # PRPG
    "Compromete Fecha": "PRPG",
    "Compromete Sin Fecha": "PRPG",
    "Compromete Fecha - Pago Parcial": "PRPG",
    "Compromete Sin Fecha - Pago Parcial": "PRPG",

    
}

def map_evento(estado):
    if estado not in ESTADO_EVENTO_MAP:
        logging.warning(f"Estado no mapeado: {estado}")
    return ESTADO_EVENTO_MAP.get(estado, "AVITITUL")


def get_estados():
    return list(ESTADO_EVENTO_MAP)

COMENTARIOS = {
    "No Cobró Sueldo": [
        "Se contacta a titular al teléfono {telefono}. Se le informa de su deuda y forma de pagar. No puede aceptar condiciones. Motivo: Aún no percibe sus haberes.",
        "Se establece comunicación al {telefono}. El cliente manifiesta voluntad de pago pero indica que no se le ha acreditado el sueldo. Motivo: Pendiente de cobro.",
        "Contacto efectivo con el titular ({telefono}). Informa que por retrasos administrativos de su empleador aún no dispone de su salario. Motivo: Sueldo no cobrado."
    ],
    "Sin Empleo": [
        "Se contacta a titular en el telefono {telefono}. Se le informa de su deuda y forma de pagar. No puede aceptar condiciones. Motivo: Actualmente desempleado.",
        "Llamada al {telefono}. El cliente explica que se encuentra sin actividad laboral y no posee ingresos fijos. Motivo: Falta de empleo.",
        "Se mantiene diálogo con el titular al {telefono}. Refiere que perdió su puesto de trabajo recientemente y no puede afrontar compromisos. Motivo: Cesante."
    ],
    "No Puede Pagar": [
        "Se contacta a titular en el telefono {telefono}. Se le informa de su deuda y forma de pagar. No puede aceptar condiciones. Motivo: Insolvencia económica.",
        "Comunicación al {telefono}. El cliente reconoce la deuda pero manifiesta una imposibilidad total de pago en este momento. Motivo: Sin recursos disponibles.",
        "Se gestiona cobro al {telefono}. El titular indica que sus gastos actuales superan sus ingresos y no puede cumplir. Motivo: Falta de capacidad de pago."
    ],
    "Vacaciones": [
        "Se logra contacto al {telefono}. El titular indica que se encuentra fuera de la ciudad por receso vacacional. Motivo: Ausencia por vacaciones.",
        "Llamada atendida al {telefono}. El cliente solicita ser contactado a su regreso ya que se encuentra de viaje. Motivo: Vacaciones.",
        "Se contacta al titular ({telefono}). Refiere estar en periodo de descanso y no tiene acceso a sus medios de pago. Motivo: Cliente de vacaciones."
    ],
    "Desconoce Deuda": [
        "Se contacta a titular en el telefono {telefono}. Se le informa de su deuda. El mismo manifiesta no reconocer el producto o el saldo. Motivo: Desconoce deuda.",
        "Comunicación al {telefono}. El cliente indica que no ha realizado consumos con la entidad y desconoce el origen del reclamo. Motivo: Error en registro o desconocimiento.",
        "El titular atiende al {telefono} y reclama que no posee deudas pendientes, solicitando revisión de cuenta. Motivo: Desconocimiento de obligación."
    ],
    "Llamada Recurrente": [
        "Se contacta al {telefono}. El titular se muestra molesto indicando que ya fue contactado previamente en el día. Motivo: Reclama reiteración de llamadas.",
        "Llamada al {telefono}. El cliente interrumpe la gestión alegando que ya brindó una respuesta en un contacto anterior. Motivo: Llamada recurrente.",
        "Se establece contacto al {telefono}. El titular refiere que la insistencia telefónica es excesiva y corta la comunicación. Motivo: Gestión repetida."
    ],
    "Falleció": [
        "Se llama al {telefono}. Atiende un familiar e informa el deceso del titular. Se solicitan datos para el área legal. Motivo: Fallecimiento.",
        "Contacto al {telefono}. Se toma conocimiento de que el cliente ha fallecido a través de un tercero en el domicilio. Motivo: Titular fallecido.",
        "Comunicación con el entorno del titular al {telefono}. Informan el fallecimiento del mismo. Motivo: Defunción."
    ],
    "Problema Económico": [
        "Se contacta a titular en el telefono {telefono}. Se le informa de su deuda. Manifiesta voluntad pero atraviesa una crisis financiera. Motivo: Problemas económicos.",
        "Llamada al {telefono}. El cliente explica que tiene otras deudas prioritarias y su flujo de caja es negativo. Motivo: Inestabilidad financiera.",
        "Se asesora al titular al {telefono}. Indica que debido a gastos imprevistos no puede cubrir la cuota este mes. Motivo: Dificultad económica."
    ],
    "Titular Cortó": [
        "Se establece contacto al {telefono}. Al momento de informar el motivo de la llamada, el titular finaliza la comunicación. Motivo: Corte abrupto.",
        "Llamada al {telefono}. El cliente atiende pero procede a colgar sin mediar palabra tras nuestra identificación. Motivo: Titular cortó llamada.",
        "Se intenta gestionar al {telefono}, pero el titular interrumpe la conexión de forma intencional. Motivo: Desconexión por parte del cliente."
    ],
    "Enfermedad": [
        "Se contacta al titular al {telefono}. Informa que por motivos de salud personales no puede realizar gestiones de pago. Motivo: Problema médico.",
        "Comunicación al {telefono}. El cliente refiere estar atravesando una situación de enfermedad familiar que consume sus recursos. Motivo: Gastos médicos / Salud.",
        "Llamada atendida al {telefono}. El titular manifiesta imposibilidad de acercarse a pagar por reposo médico. Motivo: Enfermedad."
    ],
    "Compromete Fecha": [
        "Se contacta a titular al {telefono}. Se acuerda la regularización total del saldo para la fecha estipulada. Motivo: Promesa de pago confirmada.",
        "El cliente atiende al {telefono} y confirma que el pago será acreditado el día pactado en la gestión. Motivo: Compromiso con fecha.",
        "Se establece acuerdo de pago con el titular al {telefono}. El mismo asegura contar con los fondos para la fecha acordada. Motivo: Fecha de pago pactada."
    ],
    "Compromete Sin Fecha": [
        "Se contacta al titular al {telefono}. Manifiesta que pagará a la brevedad, aunque no puede precisar un día exacto. Motivo: Compromiso de pago próximo.",
        "Comunicación al {telefono}. El cliente reconoce la deuda y afirma que enviará el pago durante la semana. Motivo: Promesa sin fecha fija.",
        "El titular ({telefono}) asegura que regularizará su situación en cuanto disponga del efectivo. Motivo: Voluntad de pago sin fecha confirmada."
    ],
    "Compromete Fecha - Pago Parcial": [
        "Se contacta al {telefono}. El titular se compromete a realizar un pago a cuenta en la fecha acordada para reducir la mora. Motivo: Pago parcial con fecha.",
        "Llamada al {telefono}. El cliente acuerda abonar una parte del total en el plazo estipulado. Motivo: Compromiso de abono parcial.",
        "Se gestiona pago parcial al {telefono}. El titular confirma el monto y la fecha en la que realizará el depósito. Motivo: Acuerdo parcial programado."
    ],
    "Compromete Sin Fecha - Pago Parcial": [
        "Se contacta al titular al {telefono}. Refiere que hará una entrega parcial de fondos en los próximos días. Motivo: Promesa de pago parcial.",
        "Comunicación al {telefono}. El cliente indica voluntad de ir reduciendo el saldo mediante pagos parciales según disponibilidad. Motivo: Abono parcial sin fecha.",
        "El titular ({telefono}) se compromete a realizar un pago a cuenta a la brevedad posible. Motivo: Compromiso parcial pendiente."
    ],
    "Ocupado": [
        "Se intenta contacto al {telefono}, pero la línea se encuentra ocupada actualmente. Se reintentará luego. Motivo: Línea ocupada.",
        "Comunicación fallida al {telefono}. El interno da señal de ocupado. Motivo: Ocupado.",
        "Intento de gestión al {telefono} sin éxito por línea ocupada. Se deja constancia para próximo barrido. Motivo: Ocupado."
    ],
    "Correo de Voz": [
        "Se establece contacto con el {telefono}, el cual deriva directamente a la casilla de mensajes. No se deja recado. Motivo: Correo de voz.",
        "Llamada al {telefono} no atendida por el titular, salta el contestador automático. Motivo: Buzón de voz.",
        "Intento de comunicación al {telefono}. La llamada es desviada al correo de voz tras varios tonos. Motivo: Contesta buzón."
    ],
    "Cortó Llamada": [
        "Se inicia el contacto al {telefono}, pero el interlocutor finaliza la llamada de forma abrupta. Motivo: Cortó comunicación.",
        "Comunicación interrumpida al {telefono} tras presentarnos. No se logra retomar el diálogo. Motivo: Colgaron llamada.",
        "Se intenta gestionar deuda al {telefono}, pero el receptor corta la comunicación inmediatamente. Motivo: Llamada cortada."
    ],
    "Número Equivocado": [
        "Se contacta al {telefono}. Atiende una persona que indica no conocer al titular. Motivo: Teléfono erróneo.",
        "Llamada al {telefono}. El interlocutor manifiesta que el número no pertenece a la persona buscada. Motivo: Número equivocado.",
        "Se establece contacto al {telefono}, pero informan que el titular ya no posee esta línea. Motivo: Datos desactualizados."
    ],
    "Indefinido": [
        "Se intenta gestionar el caso al {telefono}, pero la respuesta obtenida no es clara o la comunicación es deficiente. Motivo: Sin definición.",
        "Contacto al {telefono} con resultado ambiguo. No se logra categorizar la respuesta del cliente. Motivo: Indefinido.",
        "Llamada al {telefono}. Se percibe ruido de línea o respuesta nula por parte del receptor. Motivo: Resultado indefinido."
    ],
    "No Titular": [
        "Se contacta al {telefono}. Atiende un tercero quien informa que el titular no se encuentra disponible. Motivo: Contacto con tercero.",
        "Comunicación al {telefono}. Se habla con un allegado del cliente, quien no está autorizado a tomar compromisos. Motivo: No es el titular.",
        "Llamada atendida por otra persona al {telefono}. Se solicita al titular pero informan que no puede atender en este momento. Motivo: Tercero atiende."
    ],
    "Transferir": [
        "Se contacta al {telefono}. El cliente solicita ser derivado con un supervisor o área específica para tratar su caso. Motivo: Solicitud de transferencia.",
        "Comunicación al {telefono}. Tras explicar la deuda, el titular pide hablar con otro departamento administrativo. Motivo: Derivación solicitada.",
        "Gestión al {telefono}. Se procede a marcar para transferencia interna según requerimiento del cliente. Motivo: Transferir llamada."
    ],
    "Volver a Llamar": [
        "Se contacta al titular al {telefono}. Manifiesta estar ocupado y solicita que se le contacte en otro horario. Motivo: Volver a llamar.",
        "Llamada atendida al {telefono}. El cliente pide retomar la comunicación más tarde para analizar su situación. Motivo: Rellamar.",
        "El titular indica al {telefono} que no puede hablar en este momento. Se agenda nuevo contacto. Motivo: Solicita nueva llamada."
    ],
    "Pago Total": [
        "Se establece contacto al {telefono}. El cliente confirma que procederá a realizar el pago total del saldo pendiente. Motivo: Compromiso de pago total.",
        "Comunicación efectiva al {telefono}. El titular acepta las condiciones para cancelar la totalidad de su deuda. Motivo: Cancelación total pactada.",
        "El cliente informa al {telefono} su intención de quedar al día mediante el pago íntegro de la mora. Motivo: Pago total."
    ],
    "Refinanciar": [
        "Se contacta al titular al {telefono}. Solicita un plan de cuotas o refinanciación para poder afrontar la deuda. Motivo: Pedido de refinanciación.",
        "Llamada al {telefono}. El cliente manifiesta voluntad de pago pero requiere nuevas condiciones de financiación. Motivo: Solicita plan de pagos.",
        "El titular indica al {telefono} que solo puede cumplir mediante una reestructuración de su saldo actual. Motivo: Refinanciación."
    ],
    "Llamando": [
        "Se inicia proceso de marcación al {telefono}. Gestión en curso. Motivo: Llamando.",
        "Intento de salida de llamada al {telefono}. Esperando respuesta del suscriptor. Motivo: En llamada.",
        "Estableciendo conexión con el nro {telefono} para gestión de cobro. Motivo: Llamando."
    ]
}

def get_operador():
    return random.choice(OPERADORES)


def get_comentario(estado, index, telefono):
    # Aseguramos que el teléfono sea un string, si es Nulo ponemos vacío
    tel_str = str(telefono) if telefono and str(telefono) != 'nan' else "no registrado"
    
    lista = COMENTARIOS.get(estado, ["Gestión realizada al teléfono {telefono}"])
    
    # Seleccionamos la frase según el índice
    frase = lista[index % len(lista)]
    
    # ¡ESTA ES LA MAGIA! Reemplaza {telefono} por el número real
    return frase.format(telefono=tel_str)