create procedure raise_notice (s text) language plpgsql as 
$$
begin 
    raise notice '%', s;
end;
$$;