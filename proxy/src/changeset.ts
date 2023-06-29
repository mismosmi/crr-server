export interface Changeset {
  table: string;
  pk: any;
  cid: string | null;
  val: any;
  col_version: number;
  db_version: number;
  site_id: BigInteger;
}
