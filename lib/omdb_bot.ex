NimbleCSV.define(OMDBBot.Parser, separator: "\",", escape: "\0")

defmodule OMDBBot do
  alias OMDBBot.Parser
  alias GraphqlBuilder.Query

  def import_movies do
    parse_file()
    |> Enum.map(&create_movie/1)
  end

  def create_movie(data) do
    label = [
      value: data.name,
      language: "en-US"
    ]

    %Query{
      operation: :create_film,
      variables:
        Map.drop(data, [:name, :production_date, :omdb_id])
        |> Map.put(:label, label)
        |> Enum.into([]),
      fields: [:uid]
    }
    |> MetagraphSDK.mutate()
  end

  defp parse_file do
    Application.get_env(:omdb_bot, :file_path, "all_movies.csv")
    |> File.stream!([:trim_bom])
    # |> Stream.map(&normalize/1)
    |> Parser.parse_stream()
    |> Stream.map(fn [id, name, _parent_id, production_date] ->
      %{
        omdb_id: String.to_integer(cleanup(id)),
        name: :binary.copy(name) |> cleanup(),
        production_date: parse_date(production_date)
      }
    end)
    |> Enum.to_list()
    |> into_single_map()
    |> add_external_ids()
    |> Map.values()
  end

  defp add_external_ids(movies) do
    {_, external_links} =
      "movie_links.csv"
      |> File.read!()
      |> Parser.parse_string()
      |> Enum.map_reduce(movies, &put_values/2)

    external_links
  end

  # Add external ids
  def put_values(["\"imdbmovie", value, movie_id, _language], acc) do
    omdb_id = String.to_integer(cleanup(movie_id))
    movie = Map.get(acc, omdb_id)

    if movie do
      {
        nil,
        acc
        |> Map.put(
          omdb_id,
          Map.put(movie, :imdb_id, cleanup(value))
        )
      }
    else
      {nil, acc}
    end
  end

  def put_values(["\"wikidata", value, movie_id, _language], acc) do
    omdb_id = String.to_integer(cleanup(movie_id))
    movie = Map.get(acc, omdb_id)

    if movie do
      {
        nil,
        acc
        |> Map.put(
          omdb_id,
          Map.put(movie, :wikidata_id, cleanup(value))
        )
      }
    else
      {nil, acc}
    end
  end

  def put_values(_, acc), do: {nil, acc}

  # TODO: Fix.
  defp parse_date(_), do: nil

  defp normalize(string), do: string |> String.replace("\"", "")

  defp cleanup(string),
    do: string |> String.replace_leading("\"", "") |> String.replace("\\", "")

  defp into_single_map(movies) do
    {_, result} =
      movies
      |> Enum.map_reduce(%{}, fn x, acc ->
        {nil, Map.put(acc, x.omdb_id, x)}
      end)

    result
  end
end
