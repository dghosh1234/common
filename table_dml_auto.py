Option Explicit

Dim fso, file, filePath
Dim tbl, col
Dim dataTypeRaw, semantics, cleanLength, suggestion, outputLine

' File path on Desktop
filePath = CreateObject("WScript.Shell").SpecialFolders("Desktop") & "\varchar2_semantics_report.csv"

Set fso = CreateObject("Scripting.FileSystemObject")
Set file = fso.CreateTextFile(filePath, True)

file.WriteLine "Table Name,Column Name,Data Type,Length,Semantics,Suggestion"

For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        For Each col In tbl.Columns
            dataTypeRaw = UCase(col.DataType)

            If InStr(dataTypeRaw, "VARCHAR2") > 0 Then
                cleanLength = col.Length
                semantics = ""

                ' Extract semantics from datatype string first
                If InStr(dataTypeRaw, "CHAR") > 0 Then
                    semantics = "CHAR"
                ElseIf InStr(dataTypeRaw, "BYTE") > 0 Then
                    semantics = "BYTE"
                Else
                    ' Fallback: check extended attribute
                    On Error Resume Next
                    semantics = col.GetExtendedAttribute("LengthSemantics")
                    If Err.Number <> 0 Then semantics = "" : Err.Clear
                    On Error GoTo 0
                    semantics = UCase(Trim(semantics))
                    If semantics = "" Then semantics = "BYTE"
                End If

                ' Only flag if not already CHAR
                If semantics <> "CHAR" Then
                    suggestion = "Change to VARCHAR2(" & cleanLength & " CHAR)"
                    outputLine = """" & tbl.Code & """,""" & col.Code & """,""" & col.DataType & """,""" & cleanLength & """,""" & semantics & """,""" & suggestion & """"
                    file.WriteLine outputLine
                End If
            End If
        Next
    End If
Next

file.Close

MsgBox "✅ Report generated successfully at: " & filePath
