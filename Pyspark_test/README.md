# Apache Spark — notatki

Apache Spark to silnik do rozproszonego przetwarzania danych. Spark przetwarza dane głównie w pamięci.

## Architektura klastra

Spark działa w modelu Master-Worker.

- **Driver Node** — Główny proces aplikacji. Tworzy obiekt `SparkSession`, przekształca kod w graf operacji DAG. Planuje zadania i rozdziela je pomiędzy węzły wykonawcze.
- **Cluster Manager** — Zarządza przydzielaniem zasobów w klastrze.
- **Executorzy** — Procesy działające na węzłach typu worker. Przechowują dane w pamięci/na dyskach oraz wykonują zadania przydzielone przez Drivera.

![Arch](img/spark_arch.webp)

---

Struktura i rozmiar grafu DAG nie zależą od sprzętu, lecz wyłącznie od logiki w kodzie.

**Graf DAG (plan logiczny)** — określa co i w jakiej kolejności ma się stać z danymi (`read -> filter -> ...`). Dzieli się na etapy (Stages), których liczba zależy od tego, czy w kodzie występują operacje przetasowania danych (Shuffle, np. `groupBy`, `join`).

**Zadania (Task — fizyczna egzekucja)** — to konkretne jednostki pracy. Każdy Stage z grafu DAG jest dzielony na tyle zadań, ile jest partycji danych.

**Partycja danych** — logiczny wycinek całego zbioru danych, który znajduje się w pamięci jednej maszyny i jest przetwarzany jako jedna całość.

Przykładowo: jeśli DataFrame został podzielony na 100 partycji, a klaster ma do dyspozycji 10 rdzeni CPU, to Spark wykona 10 procesów obliczeniowych, przetwarzając po 10 partycji jednocześnie.

W przypadku dwóch maszyn 10-wątkowych, jeśli założymy, że zbiór został podzielony na 8 partycji, to Spark przydzieli 4 partycje do maszyny A i 4 do maszyny B.

---

W przypadku wczytywania danych, np. poprzez:

```python
spark.read.parquet(sciezka_do_danych)
```

nie następuje fizyczne wczytanie danych do pamięci RAM — Spark odczytuje jedynie metadane pliku (nagłówki, typy kolumn).

Następnie odbywa się podział na partycje, który odbywa się na podstawie rozmiaru pliku na dysku/w pamięci.

**Proces shuffle** w Sparku to proces przesyłania danych pomiędzy partycjami (i często między różnymi fizycznymi maszynami w sieci) tak, aby wiersze o tym samym kluczu trafiły w jedno miejsce. (Podczas shuffle dane tymczasowe są zapisywane na dysku lokalnym workera.) To najdroższa operacja w Sparku.

### Jak budowane są etapy (Stages) w DAG-u

Pojedynczy Stage tworzy się wokół granic wyznaczonych przez shuffle. Spark łączy wszystkie operacje typu Narrow (które nie wymagają przetasowania) w jeden wspólny Stage.

Przykład:

```python
df = spark.read.csv("dane.csv")               # 1.
df_filtered = df.filter(df["Pensja"] > 3000)   # 2.
df_grouped = df_filtered.groupBy("Dzial")      # 3.
df_result = df_grouped.agg({"Pensja": "avg"})  # 4.
df_result.show()                               # 5.
```

```
STAGE 1:
┌─────────────────────────────────────────────────────────────┐
│ 1. Odczyt pliku z dysku (pojedyncze wiersze)                 │
│ 2. Od razu filtrowanie (Pensja > 3000) na tej samej partycji │
│ 3. Częściowe zsumowanie i przygotowanie do wysyłki           │
└──────────────────────────────┬──────────────────────────────┘
                                │
                             SHUFFLE
                                │
                                ▼
STAGE 2:
┌─────────────────────────────────────────────────────────────┐
│ 4. Odebranie przetasowanych danych według działów            │
│ 5. Wyliczenie ostatecznej średniej dla każdego działu        │
│ 6. Zwrócenie wyników do konsoli (.show)                      │
└─────────────────────────────────────────────────────────────┘
```

---

## Pytania

### 1. RDD vs DataFrame vs Dataset

*Czym różnią się od siebie te trzy podstawowe abstrakcje danych w Apache Spark?*

> RDD to jakaś niskopoziomowa struktura danych w Sparku? DataFrame to bardziej zaawansowana struktura danych, która posiada swój schemat, jest trochę jak tabela w bazie danych. Dataset — nie wiem.

**RDD (Resilient Distributed Dataset)** — rozproszona kolekcja obiektów rozrzucona po węzłach klastra. Daje pełną, niskopoziomową kontrolę nad danymi i operacjami na poziomie pojedynczych obiektów. Obiekty RDD są jak „czarne skrzynki”, ponieważ Spark nie wie, jaka jest struktura wewnątrz takich danych, przez co zapytania nie mogą zostać zoptymalizowane na RDD.

**DataFrame** — rozproszona kolekcja danych zorganizowana w nazwane kolumny (posiada schemat). Ponieważ Spark zna nazwy i typy kolumn, widzi dane jak tabelę relacyjną. Dzięki temu silnik potrafi sam zreorganizować operacje, odrzucić niepotrzebne kolumny czy filtrować dane przed ich wczytaniem.

**Dataset** — silnie typowana wersja DataFrame, dostępna w językach kompilowanych (Scala, Java).

### 2. Lazy evaluation oraz DAG

*Co dokładnie oznacza, że Spark stosuje lazy evaluation? Jaką rolę pełnią transformacje, a jaką akcje?*

Lazy evaluation polega na tym, że gdy wywołujemy kolejne operacje na DataFrame, Spark nie zaczyna przetwarzać tych danych od razu — zamiast tego budowany jest plan, w jaki sposób te dane będą przekształcane. Gdyby Spark od razu czytał dane, wczytałby cały plik. Ponieważ np. definiujemy jakieś filtry w kodzie PySpark, silnik może już na etapie odczytu odrzucić niepotrzebne wiersze. Dodatkowo, zamiast trzymać w pamięci ogromne ilości pośrednich danych, Spark pamięta tylko schemat przekształceń — DAG.

**Transformacje** w Sparku opierają się na koncepcji lazy. Są wykorzystywane, gdy wykonujemy takie polecenia jak `filter`, `select`, `groupBy`, `join`, `withColumn`.

**Akcje** natomiast uruchamiają wykonanie naszego DAG-a i zwracają wyniki. Takie polecenia jak np. `show`, `collect`.

### 3. Narrow vs wide dependencies oraz shuffle

*Czym różni się narrow dependency od wide dependency? Co to jest proces shuffle?*

**Narrow dependencies** — dane potrzebne do wyliczenia nowej partycji znajdują się na tym samym węźle w pamięci RAM. Spark nie musi przesyłać danych przez sieć.

**Wide dependencies** — aby wyliczyć wynik (np. zsumować wartości dla danego klucza), silnik musi zebrać dane rozsiane po całym klastrze i przenieść je na jeden węzeł. Każda taka zależność wymusza podział w DAG-u i tworzy nowy Stage.

**Shuffle** — proces reorganizacji i przesyłania danych między węzłami klastra, tak aby rekordy o tym samym kluczu trafiły na tę samą partycję docelową.

### 4. Catalyst Optimizer

Silnik optymalizacyjny w Sparku. Odpowiada za optymalizację planu przetwarzania danych, np. przesuwanie filtrów (predicate pushdown).

### 5. Architektura pamięci i unikanie błędów OOM

*Jak podzielona jest pamięć na poziomie executora (storage memory vs execution memory)? Co się dzieje, gdy wykonuje się takie operacje jak `collect()` czy `groupBy()` na bardzo dużych zbiorach danych, i jak uniknąć awarii aplikacji spowodowanej brakiem pamięci (OOM)?*

```
┌────────────────────────────────────────────────────────────────────────┐
│                     PROCES JVM EXECUTORA                               │
├────────────────────────────────┬───────────────────────────────────────┤
│    Reserved Memory (300 MB)     │       Pamięć użytkowa dla Sparka      │
├────────────────────────────────┼───────────────────┬───────────────────┤
│                                  │   User Memory     │   Spark Memory    │
│  Sztywna pamięć silnika Sparka  │   (~40% domyślnie)│   (~60% domyślnie)│
│                                  │                   ├─────────┬─────────┤
│                                  │ Obiekty Pythona/  │ Storage │Execution│
│                                  │ Javy, struktury   │ Memory  │ Memory  │
│                                  │ użytkownika       │ (Cache) │(Shuffle)│
└────────────────────────────────┴───────────────────┴─────────┴─────────┘
```

**Storage Memory** — służy do trzymania zbuforowanych danych (np. po `.cache()` lub `.persist()`) oraz zmiennych rozproszonych (Broadcast Variables).

**Execution Memory** — służy do wykonywania tymczasowych obliczeń: shuffle, JOIN, groupBy, sortowanie i agregacje.

#### Zasada dynamicznego dzielenia (Dynamic Occupancy)

Granica między Storage a Execution nie jest sztywna:

- Jeśli w aplikacji nie używamy `cache()`, obszar Execution może zająć 100% pamięci Spark Memory.
- Priorytet ma Execution: jeśli Execution potrzebuje pamięci, a zajmuje ją Storage, Spark wyrzuci dane ze Storage na dysk, aby ustąpić miejsca obliczeniom shuffle.
- W drugą stronę to nie działa — Storage nie może zabrać pamięci używanej aktualnie przez obliczenia Execution.

#### Co się dzieje przy `collect()` i `groupBy()` na wielkich zbiorach?

Obie operacje mogą doprowadzić do awarii aplikacji.

**`collect()` → OOM na driverze** — ściąga wszystkie partycje ze wszystkich executorów w klastrze i próbuje zmieścić je w pamięci jednego procesu — drivera. Jeśli zbiór ma 50 GB, a pamięć drivera to 4 GB, otrzymamy:

```
java.lang.OutOfMemoryError: Java heap space
```

**`groupBy()` → OOM na executorze** — wywołuje shuffle. Wszystkie wiersze z całego klastra, które mają ten sam klucz grupowania, muszą trafić do tej samej partycji na jednym executorze. Jeśli dane są rozłożone nierównomiernie, ten jeden executor dostanie ogromną porcję danych. Zbuforowanie i posortowanie milionów rekordów dla jednego klucza przekroczy bufor execution memory i spowoduje jego awarię.

**Jak temu zapobiegać:**

- W przypadku `groupBy` można stosować Adaptive Query Execution, np. `spark.sql.adaptive.skewJoin.enabled` — Spark zidentyfikuje skośne (przeciążone) partycje i automatycznie rozbije je na mniejsze podpartycje.
- Zamiast operacji `collect()` można zapisywać dane bezpośrednio na dysku i stosować `.limit()` do podglądu danych.
