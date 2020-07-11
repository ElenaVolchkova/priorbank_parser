# Stdlib:
import codecs
import csv
import shutil
import tempfile
from copy import deepcopy
from datetime import datetime
from math import fabs
from pathlib import Path
from typing import Dict

# Django:
from django.utils.timezone import make_aware

# Thirdparty:
import dateparser

# Firstparty:
from grab_money.base.parsers.base_parser import BaseParser
from grab_money.category.models import Category
from grab_money.currency.models import Currency
from grab_money.transaction.models import Transaction


class PriorbankCSVParser(BaseParser):
    path_of_files = "grab_money/base/parsers"

    def parse(self, stream):

        OPERATION_IDENTIFIER = "Операции по"
        BLOCKED_OPERATION_IDENTIFIER = "Заблокированные суммы по"
        DELIMETER = ";"

        def line_is_start_contract(line):
            return line.startswith(OPERATION_IDENTIFIER) or line.startswith(
                BLOCKED_OPERATION_IDENTIFIER
            )

        def grab_contracts(lines):
            contracts = []
            for line in lines:
                if line_is_start_contract(line) and line not in contracts:
                    contracts += [line]
            return contracts

        def grab_header(lines):
            result = []
            for line in lines:
                if line_is_start_contract(line):
                    break
                result += [line]
            return result

        def delete_unused(lines):
            result = lines[0:2]
            for line in lines[2:]:
                data = line.split(DELIMETER)
                if line.strip():
                    res = dateparser.parse(data[0])
                    if res is None:
                        break
                    else:
                        result += [line]
                else:
                    result += [line]
            return result

        def grab_contract(lines, contract):
            result = []
            start = False
            for i, line in enumerate(lines):
                if line_is_start_contract(line):
                    if line == contract:
                        start = True
                    elif line != contract and start:
                        break
                if i == (len(lines) - 1):
                    break
                if start:
                    result += [line]
            return delete_unused(result)

        def convert_to_prefix(key):
            result = str(key).strip().split(".")[-1]
            if BLOCKED_OPERATION_IDENTIFIER in key:
                result = "B_" + result
            elif OPERATION_IDENTIFIER in key:
                result = "O_" + result
            return result

        def split_file(file_name: str) -> Dict[str, list]:
            operations = {}
            with open(file_name, encoding="windows-1251") as file:
                lines = file.readlines()
                contracts = grab_contracts(lines)
                operations["header"] = grab_header(lines)
                for contract in contracts:
                    operations[contract] = grab_contract(lines, contract)
                    # import pdb; pdb.set_trace()
            folder = (
                (x.resolve() / "..").resolve() / ("PARSED_" + x.name.split(".")[0])
            ).resolve()
            if folder.exists() and folder.is_dir():
                shutil.rmtree(folder)
            folder.mkdir()
            for key in operations.keys():
                prefix = convert_to_prefix(key)
                filename = (folder / (prefix + ".csv")).resolve()
                with open(filename, "w", encoding="windows-1251") as f:
                    if key == "header":
                        f.writelines(operations[key])
                    else:
                        f.writelines(operations[key][1:])

        def clean():
            # p = Path(".")
            p = Path(PriorbankCSVParser.path_of_files)
            for x in p.iterdir():
                if x.suffixes == [".csv"] and (
                    x.name.startswith("B_") or x.name.startswith("O_")
                ):
                    x.unlink()
                if x.is_dir() and x.name.startswith("PARSED_"):
                    shutil.rmtree(x)

        clean()

        # p = Path(".")
        p = Path(PriorbankCSVParser.path_of_files)
        for x in p.iterdir():
            if (
                x.suffixes == [".csv"]
                and not x.name.startswith("B_")
                and not x.name.startswith("O_")
            ):
                split_file(x)

        files_to_process = []

        # p = Path(".")
        p = Path(PriorbankCSVParser.path_of_files)
        for x in p.iterdir():
            if x.is_dir() and x.name.startswith("PARSED"):
                for file in x.iterdir():
                    if not file.name.startswith("header"):
                        files_to_process += [file]

        result = {}

        for file in files_to_process:
            key = file.name.split(".")[0]
            if key not in result:
                result[key] = []

            with open(file, "r", encoding="windows-1251") as f:
                lines = f.readlines()
                if not result[key]:
                    result[key] += lines[0]
                for line in lines[1:]:
                    if line.strip():
                        result[key] += line

        for key, value in result.items():
            result_file = p / (key + ".csv")
            with open(result_file, "w", encoding="windows-1251") as r:
                r.writelines(value)

        p = Path(PriorbankCSVParser.path_of_files)
        output_list = []
        for x in p.iterdir():
            card = str(x.name)[2:6]
            list_of_data = []
            data = []

            if str(x.name[0]) == "O":  # str.startswith('O')

                with open(x, "r", encoding="windows-1251") as csv_file:
                    csv_reader = csv.reader(csv_file, delimiter=";")

                    for line in csv_reader:
                        data.append(line)

                for lin in data[1:]:  # заменить на csv.DictReader (179-183)
                    index_lin = int(data.index(lin) + 1)
                    list_of_data.append(dict(zip(data[0], data[(index_lin - 1)])))
                    if index_lin == len(data) - 1:
                        break

                for row in list_of_data:

                    output = dict(
                        account_from=card,
                        account_to=row["Операция"],
                        date=datetime.strptime(
                            row["Дата транзакции"], "%d.%m.%Y %H:%M:%S"
                        ),
                        amount=fabs(
                            float((row["Сумма"]).replace(",", ".").replace(" ", ""))
                        ),
                        currency=row["Валюта"],
                        date_of_transaction=datetime.strptime(
                            row["Дата операции по счету"], "%d.%m.%Y"
                        ),
                        money_back=row["Комиссия/Money-back"],
                        account_turnover=float(
                            (row["Обороты по счету"]).replace(",", ".").replace(" ", "")
                        ),
                        operation_type=row["Категория операции"],
                    )
                    output_list.append(output)

            elif str(x.name[0]) == "B":
                with open(x, "r", encoding="windows-1251") as csv_file:
                    csv_reader = csv.reader(csv_file, delimiter=";")

                    for line in csv_reader:
                        data.append(line)

                for lin in data[1:]:
                    index_lin = int(data.index(lin) + 1)
                    list_of_data.append(dict(zip(data[0], data[(index_lin - 1)])))
                    if index_lin == len(data) - 1:
                        break

                for row in list_of_data:

                    output = dict(
                        account_from=card,
                        account_to=row["Транзакция"],
                        date=datetime.strptime(
                            row["Дата транзакции"], "%d.%m.%Y %H:%M:%S"
                        ),
                        amount=fabs(
                            float(
                                (row["Сумма транзакции"])
                                .replace(",", ".")
                                .replace(" ", "")
                            )
                        ),
                        currency=row["Валюта"],
                        operation_type=row["Категория операции"],
                    )
                    output_list.append(output)

        return output_list

    def store_transactions(self, transactions, owner):
        account_from = None
        for transaction in transactions:
            data = dict(
                # data=deepcopy(transaction),
                description=transaction["account_to"],
                keyword=transaction["account_to"],
            )

            data["owner"] = owner

            account_from = self.create_and_save_account(
                owner=owner,
                name=transaction["account_from"],
                currency=Currency.objects.get(short_unit_label=transaction["currency"]),
            )
            data["account_from"] = account_from

            category, category_account = Category.search_by_keyword(
                transaction["account_to"]
            )
            data["category"] = category
            data["account_to"] = category_account

            data["date"] = make_aware(transaction["date"])

            if transaction["currency"] == "BYN":
                data["amount"] = transaction["amount"]
            else:
                data["amount"] = Transaction.currency_exchange(
                    transaction["currency"],
                    transaction["amount"],
                    str(transaction["date"].date()),
                )

            j_date = transaction["date"].isoformat()
            # data["data"].update({"date": j_date})
            tr = Transaction(**data)
            # import pdb; pdb.set_trace()
            tr.save()
