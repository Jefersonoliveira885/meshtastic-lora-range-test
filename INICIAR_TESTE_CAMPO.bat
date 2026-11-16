@echo off
cd /d "C:\Users\olive\OneDrive\Documentos\TESTE_MESH\pacote_receptor_range_test_meshtastic"

.\.venv\Scripts\python.exe .\receptor_range_test_meshtastic.py ^
  --port COM3 ^
  --sender !0407d37c ^
  --interval 30 ^
  --count 30 ^
  --distances 0 100 200 500 750 1000 1500 2000 3000 ^
  --output-dir resultados_campo

echo.
echo O programa foi encerrado.
pause