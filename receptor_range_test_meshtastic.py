#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RECEPTOR DE ENSAIO DE DISTÂNCIA - MESHTASTIC RANGE TEST

Uso principal:
    py receptor_range_test_meshtastic.py --port COM5 --sender !a1b2c3d4

O programa:
1. conecta-se por USB ao Nó 02;
2. recebe apenas pacotes RANGE_TEST_APP do Nó 01;
3. registra todos os pacotes durante TESTE e DESLOCAMENTO;
4. usa a sequência "seq N" e o intervalo configurado no emissor;
5. cria uma janela fixa de 30 sequências em cada distância;
6. contabiliza perdas mesmo quando o primeiro ou o último pacote não chega;
7. gera:
   - pacotes_brutos.csv;
   - resultado_detalhado.csv;
   - resumo_por_distancia.csv;
   - metadados_ensaio.txt.

Observação metodológica:
O firmware Range Test envia payloads no formato "seq N". O payload não contém
o horário exato de transmissão. Portanto, o horário de envio salvo no CSV é
ESTIMADO a partir da última sequência recebida e do intervalo configurado.
O horário de recebimento, RSSI e SNR são efetivamente medidos no Nó 02.
"""

from __future__ import annotations

import argparse
import csv
import math
import re
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import fmean, stdev
from typing import Any, Optional


RANGE_TEST_PORTNUM = 66
SEQ_RE = re.compile(r"^\s*seq\s+(\d+)\s*$", re.IGNORECASE)

RAW_FIELDS = [
    "test_id",
    "indice_bruto",
    "horario_recebimento_pc_local",
    "horario_recebimento_pc_utc",
    "rx_time_radio_epoch_s",
    "rx_time_radio_local",
    "from_id",
    "from_num",
    "to_id",
    "to_num",
    "packet_id",
    "portnum",
    "payload",
    "sequencia_range",
    "estado_receptor",
    "distancia_ativa_m",
    "considerado_no_teste",
    "motivo_classificacao",
    "rx_rssi_dbm",
    "rx_snr_db",
    "hop_limit",
    "hop_start",
    "saltos_consumidos_estimados",
    "channel",
    "via_mqtt",
    "payload_bytes",
    "duplicata",
]

DETAIL_FIELDS = [
    "test_id",
    "distancia_m",
    "ordem_no_teste",
    "sequencia_range",
    "mensagem",
    "enviada",
    "recebida",
    "considerar_no_teste",
    "metodo_inicio_janela",
    "horario_envio_estimado_local",
    "horario_envio_estimado_utc",
    "horario_recebimento_pc_local",
    "horario_recebimento_pc_utc",
    "rx_time_radio_epoch_s",
    "rx_time_radio_local",
    "packet_id",
    "from_id",
    "from_num",
    "to_id",
    "to_num",
    "rx_rssi_dbm",
    "rx_snr_db",
    "hop_limit",
    "hop_start",
    "saltos_consumidos_estimados",
    "channel",
    "via_mqtt",
    "payload_bytes",
    "duplicata",
    "observacao",
]

SUMMARY_FIELDS = [
    "test_id",
    "distancia_m",
    "sequencia_inicial",
    "sequencia_final",
    "mensagens_enviadas",
    "mensagens_recebidas_unicas",
    "mensagens_perdidas",
    "pdr_percentual",
    "per_percentual",
    "sequencias_perdidas",
    "duplicatas",
    "rssi_medio_dbm",
    "rssi_min_dbm",
    "rssi_max_dbm",
    "rssi_desvio_padrao",
    "snr_medio_db",
    "snr_min_db",
    "snr_max_db",
    "snr_desvio_padrao",
    "horario_inicio_janela_estimado_local",
    "horario_fim_janela_estimado_local",
    "duracao_nominal_s",
    "motivo_encerramento",
]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(dt: Optional[datetime]) -> str:
    if dt is None:
        return ""
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def iso_local(dt: Optional[datetime]) -> str:
    if dt is None:
        return ""
    return dt.astimezone().isoformat(timespec="milliseconds")


def datetime_from_epoch_seconds(value: Any) -> Optional[datetime]:
    try:
        if value in (None, ""):
            return None
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def safe_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def safe_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def decode_payload(decoded: dict[str, Any]) -> str:
    payload = decoded.get("payload", b"")
    if isinstance(payload, bytes):
        return payload.decode("utf-8", errors="replace").strip("\x00")
    if isinstance(payload, bytearray):
        return bytes(payload).decode("utf-8", errors="replace").strip("\x00")
    if isinstance(payload, str):
        return payload.strip("\x00")

    text = decoded.get("text")
    if isinstance(text, str):
        return text
    return str(payload)


def normalize_node_id(value: str) -> str:
    value = value.strip().lower()
    if value.startswith("!"):
        hex_part = value[1:]
        int(hex_part, 16)  # valida
        return "!" + hex_part.zfill(8)
    if value.startswith("0x"):
        return "!" + f"{int(value, 16):08x}"
    if value.isdigit():
        return "!" + f"{int(value):08x}"
    raise ValueError(
        "ID inválido. Use formato !a1b2c3d4, 0xa1b2c3d4 ou número decimal."
    )


def packet_node_id(packet: dict[str, Any], id_key: str, num_key: str) -> str:
    direct = packet.get(id_key)
    if isinstance(direct, str) and direct:
        try:
            return normalize_node_id(direct)
        except ValueError:
            return direct.lower()

    number = safe_int(packet.get(num_key))
    if number is not None:
        return "!" + f"{number & 0xFFFFFFFF:08x}"
    return ""


def portnum_is_range_test(value: Any) -> bool:
    if isinstance(value, str):
        return value.upper() == "RANGE_TEST_APP"
    number = safe_int(value)
    return number == RANGE_TEST_PORTNUM


def append_csv(path: Path, fields: list[str], row: dict[str, Any]) -> None:
    new_file = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fields,
            delimiter=";",
            extrasaction="ignore",
        )
        if new_file:
            writer.writeheader()
        writer.writerow({field: row.get(field, "") for field in fields})
        f.flush()


def sample_stats(values: list[float]) -> tuple[str, str, str, str]:
    if not values:
        return "", "", "", ""
    mean_v = fmean(values)
    min_v = min(values)
    max_v = max(values)
    sd_v = stdev(values) if len(values) >= 2 else 0.0
    return (
        f"{mean_v:.3f}",
        f"{min_v:.3f}",
        f"{max_v:.3f}",
        f"{sd_v:.3f}",
    )


@dataclass
class PacketRecord:
    sequence: int
    payload: str
    received_pc_utc: datetime
    radio_rx_epoch_s: Any
    radio_rx_utc: Optional[datetime]
    packet_id: Any
    from_id: str
    from_num: Any
    to_id: str
    to_num: Any
    rssi: Any
    snr: Any
    hop_limit: Any
    hop_start: Any
    hops_used: Any
    channel: Any
    via_mqtt: bool
    payload_bytes: int
    duplicate: bool = False


@dataclass
class ActiveWindow:
    distance_m: int
    start_seq: int
    end_seq: int
    start_estimated_utc: datetime
    start_monotonic: float
    deadline_monotonic: float
    method: str
    received: dict[int, PacketRecord] = field(default_factory=dict)
    duplicate_count: int = 0
    complete_event: threading.Event = field(default_factory=threading.Event)
    finalized: bool = False


class RangeTestReceiver:
    def __init__(
        self,
        output_dir: Path,
        test_id: str,
        sender_id: str,
        interval_s: float,
        count: int,
        accept_mqtt: bool,
        timeout_margin_s: float,
    ) -> None:
        self.output_dir = output_dir
        self.test_id = test_id
        self.sender_id = sender_id
        self.interval_s = interval_s
        self.count = count
        self.accept_mqtt = accept_mqtt
        self.timeout_margin_s = timeout_margin_s

        self.raw_path = output_dir / "pacotes_brutos.csv"
        self.detail_path = output_dir / "resultado_detalhado.csv"
        self.summary_path = output_dir / "resumo_por_distancia.csv"

        self.lock = threading.RLock()
        self.raw_index = 0

        self.last_seq: Optional[int] = None
        self.last_seq_rx_monotonic: Optional[float] = None
        self.last_seq_rx_utc: Optional[datetime] = None
        self.first_valid_packet_event = threading.Event()

        self.active: Optional[ActiveWindow] = None
        self.current_state = "DESLOCAMENTO"
        self.completed_distances: list[int] = []

        self.seen_packet_keys: set[tuple[Any, int]] = set()
        self.interface: Any = None

    def sender_matches(self, packet: dict[str, Any]) -> bool:
        if not self.sender_id:
            return True
        return packet_node_id(packet, "fromId", "from") == self.sender_id

    def update_sequence_clock(
        self,
        sequence: int,
        rx_monotonic: float,
        rx_utc: datetime,
    ) -> None:
        if self.last_seq is None:
            self.last_seq = sequence
            self.last_seq_rx_monotonic = rx_monotonic
            self.last_seq_rx_utc = rx_utc
            self.first_valid_packet_event.set()
            return

        if sequence > self.last_seq:
            self.last_seq = sequence
            self.last_seq_rx_monotonic = rx_monotonic
            self.last_seq_rx_utc = rx_utc
            return

        # Possível reinicialização do emissor: a sequência volta para um valor baixo.
        if self.last_seq - sequence > 1000:
            print(
                "\nATENÇÃO: possível reinicialização do Nó 01 detectada "
                f"(seq {self.last_seq} -> {sequence})."
            )
            if self.active and not self.active.finalized:
                print(
                    "O ponto atual será encerrado como REINICIALIZACAO_EMISSOR. "
                    "Repita esse ponto depois."
                )
                self.finalize_active("REINICIALIZACAO_EMISSOR")
            self.last_seq = sequence
            self.last_seq_rx_monotonic = rx_monotonic
            self.last_seq_rx_utc = rx_utc
            self.first_valid_packet_event.set()

    def estimate_next_sequence(
        self,
        press_monotonic: float,
        press_utc: datetime,
    ) -> tuple[int, datetime, str]:
        if (
            self.last_seq is None
            or self.last_seq_rx_monotonic is None
            or self.last_seq_rx_utc is None
        ):
            raise RuntimeError(
                "Ainda não existe sequência de referência. "
                "Aguarde o recebimento de pelo menos um pacote do Nó 01."
            )

        elapsed = max(0.0, press_monotonic - self.last_seq_rx_monotonic)

        # Pequena margem evita escolher uma sequência cujo instante já passou
        # no exato momento em que o usuário pressionou Enter.
        phase_guard_s = min(1.0, self.interval_s * 0.05)
        steps = max(1, math.ceil((elapsed + phase_guard_s) / self.interval_s))

        start_seq = self.last_seq + steps
        start_utc = self.last_seq_rx_utc + timedelta(
            seconds=steps * self.interval_s
        )
        method = (
            "ESTIMADO_PELA_ULTIMA_SEQUENCIA_RECEBIDA_E_INTERVALO_CONFIGURADO"
        )
        return start_seq, start_utc, method

    def arm_distance(self, distance_m: int) -> ActiveWindow:
        with self.lock:
            if self.active and not self.active.finalized:
                raise RuntimeError("Já existe um ponto em andamento.")

            press_mono = time.monotonic()
            press_utc = now_utc()
            start_seq, start_utc, method = self.estimate_next_sequence(
                press_mono,
                press_utc,
            )
            end_seq = start_seq + self.count - 1

            # O limite permite observar um pacote posterior caso a última
            # sequência da janela seja perdida.
            deadline = press_mono + (
                (self.count + 2) * self.interval_s + self.timeout_margin_s
            )

            self.active = ActiveWindow(
                distance_m=distance_m,
                start_seq=start_seq,
                end_seq=end_seq,
                start_estimated_utc=start_utc,
                start_monotonic=press_mono,
                deadline_monotonic=deadline,
                method=method,
            )
            self.current_state = "TESTE"

            print("\n" + "=" * 78)
            print(f"TESTE INICIADO: {distance_m} m")
            print(f"Janela fixa: seq {start_seq} até seq {end_seq}")
            print(f"Transmissões previstas: {self.count}")
            print(
                "Início estimado da primeira transmissão: "
                f"{iso_local(start_utc)}"
            )
            print(
                "Não movimente o Nó 02 até o programa informar que o ponto terminou."
            )
            print("=" * 78)
            return self.active

    def packet_to_record(
        self,
        packet: dict[str, Any],
        sequence: int,
        payload: str,
        rx_pc_utc: datetime,
        duplicate: bool,
    ) -> PacketRecord:
        radio_epoch = packet.get("rxTime")
        radio_dt = datetime_from_epoch_seconds(radio_epoch)

        hop_limit = safe_int(packet.get("hopLimit"))
        hop_start = safe_int(packet.get("hopStart"))
        hops_used: Any = ""
        if hop_limit is not None and hop_start is not None:
            hops_used = max(0, hop_start - hop_limit)

        payload_bytes = len(payload.encode("utf-8", errors="replace"))

        return PacketRecord(
            sequence=sequence,
            payload=payload,
            received_pc_utc=rx_pc_utc,
            radio_rx_epoch_s=radio_epoch if radio_epoch is not None else "",
            radio_rx_utc=radio_dt,
            packet_id=packet.get("id", ""),
            from_id=packet_node_id(packet, "fromId", "from"),
            from_num=packet.get("from", ""),
            to_id=packet_node_id(packet, "toId", "to"),
            to_num=packet.get("to", ""),
            rssi=packet.get("rxRssi", ""),
            snr=packet.get("rxSnr", ""),
            hop_limit=packet.get("hopLimit", ""),
            hop_start=packet.get("hopStart", ""),
            hops_used=hops_used,
            channel=packet.get("channel", ""),
            via_mqtt=bool(packet.get("viaMqtt", False)),
            payload_bytes=payload_bytes,
            duplicate=duplicate,
        )

    def write_raw(
        self,
        record: PacketRecord,
        state: str,
        distance: Any,
        considered: str,
        reason: str,
    ) -> None:
        self.raw_index += 1
        row = {
            "test_id": self.test_id,
            "indice_bruto": self.raw_index,
            "horario_recebimento_pc_local": iso_local(record.received_pc_utc),
            "horario_recebimento_pc_utc": iso_utc(record.received_pc_utc),
            "rx_time_radio_epoch_s": record.radio_rx_epoch_s,
            "rx_time_radio_local": iso_local(record.radio_rx_utc),
            "from_id": record.from_id,
            "from_num": record.from_num,
            "to_id": record.to_id,
            "to_num": record.to_num,
            "packet_id": record.packet_id,
            "portnum": "RANGE_TEST_APP",
            "payload": record.payload,
            "sequencia_range": record.sequence,
            "estado_receptor": state,
            "distancia_ativa_m": distance,
            "considerado_no_teste": considered,
            "motivo_classificacao": reason,
            "rx_rssi_dbm": record.rssi,
            "rx_snr_db": record.snr,
            "hop_limit": record.hop_limit,
            "hop_start": record.hop_start,
            "saltos_consumidos_estimados": record.hops_used,
            "channel": record.channel,
            "via_mqtt": "SIM" if record.via_mqtt else "NAO",
            "payload_bytes": record.payload_bytes,
            "duplicata": "SIM" if record.duplicate else "NAO",
        }
        append_csv(self.raw_path, RAW_FIELDS, row)

    def on_receive(self, packet: dict[str, Any], interface: Any) -> None:
        try:
            decoded = packet.get("decoded", {}) or {}
            portnum = decoded.get("portnum")
            if not portnum_is_range_test(portnum):
                return

            if not self.sender_matches(packet):
                return

            via_mqtt = bool(packet.get("viaMqtt", False))
            if via_mqtt and not self.accept_mqtt:
                return

            payload = decode_payload(decoded)
            match = SEQ_RE.match(payload)
            if not match:
                return

            sequence = int(match.group(1))
            rx_pc_utc = now_utc()
            rx_mono = time.monotonic()

            packet_key = (packet.get("id", ""), sequence)
            duplicate = packet_key in self.seen_packet_keys
            self.seen_packet_keys.add(packet_key)

            record = self.packet_to_record(
                packet,
                sequence,
                payload,
                rx_pc_utc,
                duplicate,
            )

            with self.lock:
                # Atualiza a referência de sequência mesmo durante deslocamentos.
                self.update_sequence_clock(sequence, rx_mono, rx_pc_utc)

                active = self.active
                state = self.current_state
                distance: Any = (
                    active.distance_m
                    if active is not None and not active.finalized
                    else ""
                )

                if active is None or active.finalized:
                    self.write_raw(
                        record,
                        state="DESLOCAMENTO",
                        distance="",
                        considered="NAO",
                        reason="FORA_DE_JANELA_DE_TESTE",
                    )
                    print(
                        f"[DESLOCAMENTO] seq={sequence} | "
                        f"RSSI={record.rssi} dBm | SNR={record.snr} dB"
                    )
                    return

                if sequence < active.start_seq:
                    self.write_raw(
                        record,
                        state="TESTE",
                        distance=active.distance_m,
                        considered="NAO",
                        reason="ANTERIOR_A_JANELA_FIXA",
                    )
                    print(
                        f"[PRÉ-JANELA {active.distance_m} m] seq={sequence}; "
                        f"início previsto={active.start_seq}"
                    )
                    return

                if sequence > active.end_seq:
                    # Um pacote posterior prova que a janela terminou, mesmo se
                    # a última sequência esperada tiver sido perdida.
                    self.finalize_active("SEQUENCIA_POSTERIOR_RECEBIDA")
                    self.write_raw(
                        record,
                        state="DESLOCAMENTO",
                        distance="",
                        considered="NAO",
                        reason="POSTERIOR_A_JANELA_ENCERRADA",
                    )
                    print(
                        f"[PÓS-JANELA] seq={sequence} não pertence ao ponto encerrado."
                    )
                    return

                # Pacote está dentro da janela fixa.
                if sequence in active.received:
                    active.duplicate_count += 1
                    record.duplicate = True
                    self.write_raw(
                        record,
                        state="TESTE",
                        distance=active.distance_m,
                        considered="NAO",
                        reason="DUPLICATA_DENTRO_DA_JANELA",
                    )
                    print(
                        f"[DUPLICATA {active.distance_m} m] seq={sequence}"
                    )
                    return

                active.received[sequence] = record
                self.write_raw(
                    record,
                    state="TESTE",
                    distance=active.distance_m,
                    considered="SIM",
                    reason="PACOTE_UNICO_DENTRO_DA_JANELA",
                )

                order = sequence - active.start_seq + 1
                print(
                    f"[TESTE {active.distance_m:4d} m] "
                    f"seq={sequence} ({order:02d}/{self.count}) | "
                    f"RSSI={record.rssi} dBm | SNR={record.snr} dB | "
                    f"hora={iso_local(rx_pc_utc)}"
                )

                if sequence == active.end_seq:
                    self.finalize_active("ULTIMA_SEQUENCIA_RECEBIDA")

        except Exception as exc:
            print(f"\nERRO ao processar pacote: {exc!r}", file=sys.stderr)

    def finalize_active(self, reason: str) -> None:
        with self.lock:
            active = self.active
            if active is None or active.finalized:
                return

            active.finalized = True
            self.current_state = "DESLOCAMENTO"

            lost_sequences: list[int] = []
            detail_rows: list[dict[str, Any]] = []

            for order, sequence in enumerate(
                range(active.start_seq, active.end_seq + 1),
                start=1,
            ):
                expected_utc = active.start_estimated_utc + timedelta(
                    seconds=(order - 1) * self.interval_s
                )
                record = active.received.get(sequence)

                if record is None:
                    lost_sequences.append(sequence)
                    detail_rows.append(
                        {
                            "test_id": self.test_id,
                            "distancia_m": active.distance_m,
                            "ordem_no_teste": order,
                            "sequencia_range": sequence,
                            "mensagem": f"seq {sequence}",
                            "enviada": "SIM",
                            "recebida": "NAO",
                            "considerar_no_teste": "SIM",
                            "metodo_inicio_janela": active.method,
                            "horario_envio_estimado_local": iso_local(expected_utc),
                            "horario_envio_estimado_utc": iso_utc(expected_utc),
                            "duplicata": "",
                            "observacao": (
                                "PACOTE_NAO_RECEBIDO; CAMPOS_DE_RF_E_RECEPCAO_VAZIOS"
                            ),
                        }
                    )
                    continue

                detail_rows.append(
                    {
                        "test_id": self.test_id,
                        "distancia_m": active.distance_m,
                        "ordem_no_teste": order,
                        "sequencia_range": sequence,
                        "mensagem": record.payload,
                        "enviada": "SIM",
                        "recebida": "SIM",
                        "considerar_no_teste": "SIM",
                        "metodo_inicio_janela": active.method,
                        "horario_envio_estimado_local": iso_local(expected_utc),
                        "horario_envio_estimado_utc": iso_utc(expected_utc),
                        "horario_recebimento_pc_local": iso_local(
                            record.received_pc_utc
                        ),
                        "horario_recebimento_pc_utc": iso_utc(
                            record.received_pc_utc
                        ),
                        "rx_time_radio_epoch_s": record.radio_rx_epoch_s,
                        "rx_time_radio_local": iso_local(record.radio_rx_utc),
                        "packet_id": record.packet_id,
                        "from_id": record.from_id,
                        "from_num": record.from_num,
                        "to_id": record.to_id,
                        "to_num": record.to_num,
                        "rx_rssi_dbm": record.rssi,
                        "rx_snr_db": record.snr,
                        "hop_limit": record.hop_limit,
                        "hop_start": record.hop_start,
                        "saltos_consumidos_estimados": record.hops_used,
                        "channel": record.channel,
                        "via_mqtt": "SIM" if record.via_mqtt else "NAO",
                        "payload_bytes": record.payload_bytes,
                        "duplicata": "NAO",
                        "observacao": (
                            "HORARIO_DE_ENVIO_ESTIMADO; "
                            "HORARIO_DE_RECEPCAO_E_METRICAS_MEDIDOS"
                        ),
                    }
                )

            for row in detail_rows:
                append_csv(self.detail_path, DETAIL_FIELDS, row)

            received_records = list(active.received.values())
            rssis = [
                value
                for value in (safe_float(r.rssi) for r in received_records)
                if value is not None
            ]
            snrs = [
                value
                for value in (safe_float(r.snr) for r in received_records)
                if value is not None
            ]

            rssi_mean, rssi_min, rssi_max, rssi_sd = sample_stats(rssis)
            snr_mean, snr_min, snr_max, snr_sd = sample_stats(snrs)

            received_count = len(active.received)
            lost_count = self.count - received_count
            pdr = received_count / self.count * 100.0
            per = lost_count / self.count * 100.0

            end_estimated_utc = active.start_estimated_utc + timedelta(
                seconds=(self.count - 1) * self.interval_s
            )

            summary_row = {
                "test_id": self.test_id,
                "distancia_m": active.distance_m,
                "sequencia_inicial": active.start_seq,
                "sequencia_final": active.end_seq,
                "mensagens_enviadas": self.count,
                "mensagens_recebidas_unicas": received_count,
                "mensagens_perdidas": lost_count,
                "pdr_percentual": f"{pdr:.2f}",
                "per_percentual": f"{per:.2f}",
                "sequencias_perdidas": ",".join(
                    str(seq) for seq in lost_sequences
                ),
                "duplicatas": active.duplicate_count,
                "rssi_medio_dbm": rssi_mean,
                "rssi_min_dbm": rssi_min,
                "rssi_max_dbm": rssi_max,
                "rssi_desvio_padrao": rssi_sd,
                "snr_medio_db": snr_mean,
                "snr_min_db": snr_min,
                "snr_max_db": snr_max,
                "snr_desvio_padrao": snr_sd,
                "horario_inicio_janela_estimado_local": iso_local(
                    active.start_estimated_utc
                ),
                "horario_fim_janela_estimado_local": iso_local(
                    end_estimated_utc
                ),
                "duracao_nominal_s": (self.count - 1) * self.interval_s,
                "motivo_encerramento": reason,
            }
            append_csv(self.summary_path, SUMMARY_FIELDS, summary_row)

            self.completed_distances.append(active.distance_m)
            active.complete_event.set()

            print("\n" + "#" * 78)
            print(f"PONTO DE {active.distance_m} m CONCLUÍDO")
            print(
                f"Janela: seq {active.start_seq} até seq {active.end_seq}"
            )
            print(
                f"Recebidas: {received_count}/{self.count} | "
                f"Perdidas: {lost_count} | PDR: {pdr:.2f}% | PER: {per:.2f}%"
            )
            print(
                "Sequências perdidas: "
                + (", ".join(map(str, lost_sequences)) if lost_sequences else "nenhuma")
            )
            print("Agora você pode iniciar o deslocamento para o próximo ponto.")
            print("#" * 78 + "\n")

    def wait_for_window(self, active: ActiveWindow) -> None:
        while not active.complete_event.wait(timeout=1.0):
            if time.monotonic() >= active.deadline_monotonic:
                self.finalize_active("TIMEOUT_DA_JANELA")
                break

    def write_metadata(
        self,
        args: argparse.Namespace,
        started_at: datetime,
    ) -> None:
        metadata_path = self.output_dir / "metadados_ensaio.txt"
        content = f"""METADADOS DO ENSAIO MESHTASTIC RANGE TEST

ID do ensaio: {self.test_id}
Início local: {iso_local(started_at)}
Início UTC: {iso_utc(started_at)}
Porta serial do Nó 02: {args.port or "detecção automática"}
ID filtrado do Nó 01: {self.sender_id or "sem filtro"}
Intervalo configurado no emissor: {self.interval_s} s
Mensagens por distância: {self.count}
Distâncias programadas: {", ".join(map(str, args.distances))} m
Aceitar MQTT: {"SIM" if self.accept_mqtt else "NÃO"}
Margem adicional de timeout: {self.timeout_margin_s} s

ARQUIVOS
- pacotes_brutos.csv: todos os pacotes RANGE_TEST recebidos, inclusive deslocamento.
- resultado_detalhado.csv: uma linha para cada transmissão esperada.
- resumo_por_distancia.csv: PDR, PER, RSSI, SNR e perdas por distância.

LIMITAÇÃO TEMPORAL
O Range Test envia payload "seq N", sem timestamp do transmissor.
O horário de envio é estimado pela sequência e pelo intervalo.
O horário de recepção, RSSI e SNR são medidos no Nó 02.
"""
        metadata_path.write_text(content, encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Registra ensaio Range Test Meshtastic em janelas fixas "
            "de sequências por distância."
        )
    )
    parser.add_argument(
        "--port",
        default="",
        help=(
            "Porta serial do Nó 02, por exemplo COM5 ou /dev/ttyUSB0. "
            "Se omitida, a biblioteca tentará detectar automaticamente."
        ),
    )
    parser.add_argument(
        "--sender",
        default="",
        help=(
            "ID do Nó 01 emissor, por exemplo !a1b2c3d4. "
            "É recomendado informar para ignorar outros emissores."
        ),
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=30.0,
        help="Intervalo do Range Test no Nó 01, em segundos. Padrão: 30.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=30,
        help="Quantidade de sequências transmitidas por distância. Padrão: 30.",
    )
    parser.add_argument(
        "--distances",
        nargs="+",
        type=int,
        default=[0, 100, 200, 500, 750, 1000, 1500, 2000, 3000],
        help="Distâncias em metros.",
    )
    parser.add_argument(
        "--output-dir",
        default="resultados_range_test",
        help="Pasta onde os CSVs serão criados.",
    )
    parser.add_argument(
        "--test-id",
        default="",
        help="Identificação do ensaio. Se omitida, será gerada pela data/hora.",
    )
    parser.add_argument(
        "--accept-mqtt",
        action="store_true",
        help=(
            "Aceita pacotes marcados como MQTT. Não use em ensaio LoRa puro."
        ),
    )
    parser.add_argument(
        "--timeout-margin",
        type=float,
        default=90.0,
        help="Margem adicional para encerrar cada janela, em segundos.",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Executa testes internos do parser sem conectar ao rádio.",
    )
    return parser


def run_self_test() -> int:
    assert SEQ_RE.match("seq 1")
    assert SEQ_RE.match("SEQ 123")
    assert not SEQ_RE.match("teste 123")
    assert portnum_is_range_test("RANGE_TEST_APP")
    assert portnum_is_range_test(66)
    assert not portnum_is_range_test(1)
    assert normalize_node_id("!abc") == "!00000abc"
    assert normalize_node_id("0xabcdef01") == "!abcdef01"
    assert normalize_node_id(str(int("abcdef01", 16))) == "!abcdef01"
    print("SELF-TEST: todos os testes internos foram aprovados.")
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.self_test:
        return run_self_test()

    if args.interval <= 0:
        parser.error("--interval deve ser maior que zero.")
    if args.count <= 0:
        parser.error("--count deve ser maior que zero.")
    if not args.distances:
        parser.error("Informe pelo menos uma distância.")
    if args.timeout_margin < 0:
        parser.error("--timeout-margin não pode ser negativo.")

    try:
        sender_id = normalize_node_id(args.sender) if args.sender else ""
    except ValueError as exc:
        parser.error(str(exc))

    test_id = (
        args.test_id.strip()
        if args.test_id.strip()
        else now_utc().strftime("RT_%Y%m%d_%H%M%S")
    )
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    receiver = RangeTestReceiver(
        output_dir=output_dir,
        test_id=test_id,
        sender_id=sender_id,
        interval_s=args.interval,
        count=args.count,
        accept_mqtt=args.accept_mqtt,
        timeout_margin_s=args.timeout_margin,
    )

    started_at = now_utc()
    receiver.write_metadata(args, started_at)

    try:
        import meshtastic.serial_interface
        from pubsub import pub
    except ImportError:
        print(
            "ERRO: biblioteca Meshtastic não instalada.\n"
            'Execute: py -m pip install --upgrade "meshtastic[cli]"',
            file=sys.stderr,
        )
        return 2

    pub.subscribe(receiver.on_receive, "meshtastic.receive")

    print("\nConectando ao Nó 02...")
    try:
        if args.port:
            interface = meshtastic.serial_interface.SerialInterface(
                devPath=args.port
            )
        else:
            interface = meshtastic.serial_interface.SerialInterface()
    except Exception as exc:
        print(f"Não foi possível conectar ao Nó 02: {exc}", file=sys.stderr)
        return 3

    receiver.interface = interface

    print("Conexão estabelecida.")
    print(f"ID do ensaio: {test_id}")
    print(f"Pasta de saída: {output_dir}")
    print(
        "Filtro do Nó 01: "
        + (sender_id if sender_id else "DESATIVADO (não recomendado)")
    )
    print(
        f"Configuração do ensaio: {args.count} mensagens, "
        f"intervalo de {args.interval:g} s."
    )
    print(
        "Aguardando um pacote RANGE_TEST_APP válido para sincronizar "
        "a sequência do emissor..."
    )

    try:
        while not receiver.first_valid_packet_event.wait(timeout=1.0):
            pass

        print(
            f"Sincronização obtida. Última sequência recebida: "
            f"{receiver.last_seq}"
        )

        for distance in args.distances:
            print("\n" + "-" * 78)
            print(f"PRÓXIMO PONTO: {distance} m")
            print(
                "1. Posicione o Nó 02 exatamente no ponto.\n"
                "2. Mantenha antena, altura e orientação definidas.\n"
                "3. Pare o deslocamento.\n"
                "4. Pressione ENTER para iniciar a janela de 30 transmissões."
            )
            command = input(
                "Pressione ENTER para iniciar ou digite Q para encerrar: "
            ).strip().lower()
            if command == "q":
                print("Ensaio encerrado pelo usuário.")
                break

            active = receiver.arm_distance(distance)
            receiver.wait_for_window(active)

        print("\nEnsaio finalizado.")
        print(f"Pacotes brutos: {receiver.raw_path}")
        print(f"Resultado detalhado: {receiver.detail_path}")
        print(f"Resumo: {receiver.summary_path}")
        print(
            "Desative o Range Test no Nó 01 e coloque Sender Interval = 0."
        )
        return 0

    except KeyboardInterrupt:
        print("\nInterrupção solicitada pelo usuário.")
        if receiver.active and not receiver.active.finalized:
            receiver.finalize_active("INTERRUPCAO_MANUAL")
        return 130
    finally:
        try:
            interface.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
