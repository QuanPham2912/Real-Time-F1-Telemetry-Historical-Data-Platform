from enum import Enum

class F1ResultStatus(str,Enum):
    FINISHED = "FINISHED"
    DNF = "DNF"
    DSQ = "DSQ"
    DNS = "DNS"
    NC = "NC"
    DNQ = "DNQ"
    DNPQ = "DNPQ"
    EXCLUDED = "EXCLUDED"
    WITHDRAWAL = "WITHDRAWAL"

    @classmethod
    def get_code_mapping(cls) -> dict:
        return{
            "ab" : cls.DNF,
            "nc" : cls.NC,
            "np" : cls.DNS,
            "nq" : cls.DNQ,
            "npq" : cls.DNPQ,
            "dsq" : cls.DSQ,
            "exc" : cls.EXCLUDED,
            "f" : cls.WITHDRAWAL
        }
